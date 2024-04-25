
use derivative::Derivative;
use aptos_types::block_metadata::BlockMetadata;
use std::sync::Arc;
use tokio::sync::{mpsc::Sender, RwLock};
use aptos_api::Context;
use aptos_config::config::NodeConfig;
use aptos_crypto::HashValue;
use aptos_db::AptosDB;
use aptos_executor::block_executor::BlockExecutor;
use aptos_executor::db_bootstrapper::{generate_waypoint, maybe_bootstrap};
use aptos_executor_types::BlockExecutorTrait;
use aptos_mempool::core_mempool::CoreMempool;
use aptos_mempool::{MempoolClientRequest, MempoolClientSender, SubmissionStatus};
use aptos_storage_interface::DbReaderWriter;
use aptos_types::account_address::AccountAddress;
use aptos_types::block_executor::partitioner::{ExecutableBlock, ExecutableTransactions};
use aptos_types::block_info::BlockInfo;
use aptos_types::chain_id::ChainId;
use aptos_types::ledger_info::{generate_ledger_info_with_sig, LedgerInfo};
use aptos_types::mempool_status::{MempoolStatus, MempoolStatusCode};
use aptos_types::transaction::{Transaction, WriteSetPayload};
use aptos_types::validator_signer::ValidatorSigner;
use aptos_vm::AptosVM;
use aptos_vm_genesis::test_genesis_change_set_and_validators;
use tokio::task::JoinHandle;
use crate::types::Block;
use futures::StreamExt;

#[async_trait::async_trait]
pub trait ExecutorOperations {

    /// Initializes the executor
    async fn init(&self, config: NodeConfig) -> Result<(), anyhow::Error>;

    /// Executes a block
    async fn execute_block(&self, block: Block) -> Result<(), anyhow::Error>;

    /// Gets the api service
    async fn get_api_context(&self) -> Result<Context, anyhow::Error>;

    /// Waits for a transaction to be executed and signals back to the receiver
    async fn receive_executed_transaction(&self, transaction: Transaction, sender : Sender<Transaction>) -> Result<(), anyhow::Error>;

    /// Gets the db reader
    async fn get_db_reader(&self) -> Result<DbReaderWriter, anyhow::Error>;

}

#[derive(Debug, Clone)]
pub enum ExecutorState {
    Init(Init),
    Ready(Ready),
    Unhealthy(Unhealthy),
}

impl ExecutorState {

    pub fn new() -> Self {
        ExecutorState::Init(Init)
    }

    pub async fn init(&self, config: NodeConfig) -> Result<Ready, anyhow::Error> {
        match self {
            ExecutorState::Init(init) => {
                init.init(config).await
            },
            _ => {
                anyhow::bail!("Executor is not in init state");
            },
        }
    }

    pub async fn execute_block(&self, block: Block) -> Result<(), anyhow::Error> {
        match self {
            ExecutorState::Ready(ready) => {
               ready.execute_block(block).await
            },
            _ => {
                anyhow::bail!("Executor is not in ready state");
            },
        }
    }

    pub async fn get_api_context(&self) -> Result<Context, anyhow::Error> {
        match self {
            ExecutorState::Ready(ready) => {
                Ok(ready.api_context.clone())
            },
            _ => {
                anyhow::bail!("Executor is not in ready state");
            },
        }
    }

    pub async fn get_db_reader(&self) -> Result<DbReaderWriter, anyhow::Error> {
        match self {
            ExecutorState::Ready(ready) => {
                let db = ready.db.read().await;
                Ok(db.clone())
            },
            _ => {
                anyhow::bail!("Executor is not in ready state");
            },
        }
    }



}

#[derive(Debug, Clone)]
pub struct Init;

impl Init {

    pub async fn init(&self, config: NodeConfig) -> Result<Ready, anyhow::Error> {

        // initialize the mempool object
        let core_mempool = Arc::new(RwLock::new(CoreMempool::new(&config)));
    
        // initialize the est Genesis
        let (genesis, validators) = test_genesis_change_set_and_validators(Some(1));
        let signer = ValidatorSigner::new(
            validators[0].data.owner_address,
            validators[0].consensus_key.clone(),
        );
        let genesis_txn = Transaction::GenesisTransaction(WriteSetPayload::Direct(genesis));

        // configure db
        let home = match dirs::home_dir() {
            Some(path) => path,
            None => {
                anyhow::bail!("Cannot find home directory");
            }
        };
    
        let path = home.join(config.get_data_dir());
        tokio::fs::create_dir_all(path.clone()).await?;
        let (_aptos_db, db_reader_writer) = DbReaderWriter::wrap(
            AptosDB::new_for_test(path.clone())
        );
        let db = Arc::new(RwLock::new(db_reader_writer.clone()));

        // generate the waypoint
        match generate_waypoint::<AptosVM>(&db_reader_writer, &genesis_txn) {
            Ok(waypoint) => {
                maybe_bootstrap::<AptosVM>(&db_reader_writer, &genesis_txn, waypoint).unwrap();
            },
            _ => {},
        }
     
        // initialize the executor
        let block_executor = BlockExecutor::new(db_reader_writer.clone());
        let executor = Arc::new(RwLock::new(block_executor));

        // initialize the mempool
        let (mempool_client_sender, mut mempool_client_receiver) =
            futures::channel::mpsc::channel::<MempoolClientRequest>(10);
        let sender = MempoolClientSender::from(mempool_client_sender);

         // set up the api context
         let api_context = Context::new(
            ChainId::test(),
            db_reader_writer.reader.clone(),
            sender,
            config.clone(),
        );

        // spawn the mempool handler
        let mempool_handler = Arc::new(tokio::task::spawn(async move {
            while let Some(request) = mempool_client_receiver.next().await {
                match request {
                    MempoolClientRequest::SubmitTransaction(_t, callback) => {
                        // accept all the transaction
                        let ms = MempoolStatus::new(MempoolStatusCode::Accepted);
                        let status: SubmissionStatus = (ms, None);
                        callback.send(Ok(status)).unwrap();
                    },
                    MempoolClientRequest::GetTransactionByHash(_, _) => {},
                }
            }
        }));

        Ok(Ready {
            api_context,
            core_mempool,
            mempool_handler,
            db,
            signer,
            executor,
        })


    }

}

#[derive(Derivative)]
#[derivative(Debug)]
#[derive(Clone)]
pub struct Ready {

    pub api_context: Context,

    #[derivative(Debug="ignore")]
    pub core_mempool: Arc<RwLock<CoreMempool>>,

    pub mempool_handler: Arc<JoinHandle<()>>,

    #[derivative(Debug="ignore")]
    pub db: Arc<RwLock<DbReaderWriter>>,

    pub signer: ValidatorSigner,

    #[derivative(Debug="ignore")]
    pub executor: Arc<RwLock<BlockExecutor<AptosVM>>>,

}

impl Ready {

    pub async fn execute_block(&self, block: Block) -> Result<(), anyhow::Error> {
        
        // get block and timestamp
        let ts = block.ts;
        let block_id = block.hash_value()?;

        // get next epoch
        let db = self.db.read().await;
        let latest_ledger_info = db.reader.get_latest_ledger_info()?;
        let next_epoch = latest_ledger_info.ledger_info().next_block_epoch();

        // Build block metadata
        let block_meta = Transaction::BlockMetadata(BlockMetadata::new(
            block_id,
            next_epoch,
            0,
            self.signer.author(),
            vec![],
            vec![],
            ts,
        ));

        // build checkpoint
        let checkpoint = Transaction::StateCheckpoint(HashValue::random());

        // build the block
        let mut inner_block = vec![];
        inner_block.push(block_meta);
        inner_block.extend(block.transactions.clone());
        inner_block.push(checkpoint);
        let executable_transactions = ExecutableTransactions::Unsharded(
            inner_block
        );
        let executable_block = ExecutableBlock::new(
            block.hash_value()?,
            executable_transactions,
        );

        // execute the block
        let executor = self.executor.write().await;
        let parent_block_id = executor.committed_block_id();
        let output = executor.execute_block(
            executable_block,
            parent_block_id,
            None
        )?;

        // commit the ledger info
        let ledger_info = LedgerInfo::new(
            BlockInfo::new(
                next_epoch,
                0,
                block_id,
                output.root_hash(),
                output.version(),
                ts,
                output.epoch_state().clone(),
            ),
            HashValue::zero(),
        );
        let signed_ledger_info = generate_ledger_info_with_sig(
            &[self.signer.clone()],
            ledger_info,
        );
        executor.commit_blocks(
            vec![block_id], 
            signed_ledger_info.clone()
        )?;

        // commit the transactions
        {
            let mut mempool = self.core_mempool.write().await;
                for transaction in block.transactions {
                    match transaction {
                        Transaction::UserTransaction(txn) => {
                            let sender = txn.sender();
                            let sequence_number = txn.sequence_number();
                            mempool.commit_transaction(&AccountAddress::from(sender), sequence_number);
                        },
                        _ => {},
                    
                }
            }

        }

        Ok(())

    }


}

#[derive(Debug, Clone)]
pub struct Unhealthy;

#[derive(Debug, Clone)]
pub struct Executor {

    pub state: Arc<RwLock<ExecutorState>>,

}

impl Executor {

    pub fn new() -> Self {
        Executor {
            state: Arc::new(RwLock::new(ExecutorState::new())),
        }
    }

}

#[async_trait::async_trait]
impl ExecutorOperations for Executor {

    async fn init(&self, config: NodeConfig) -> Result<(), anyhow::Error> {
        let mut state = self.state.write().await;
        *state = ExecutorState::Ready(state.init(config).await?);
        Ok(())
    }

    async fn execute_block(&self, block: Block) -> Result<(), anyhow::Error> {
        let state = self.state.read().await;
        state.execute_block(block).await?;
        Ok(())
    }

    async fn get_api_context(&self) -> Result<Context, anyhow::Error> {
        let state = self.state.read().await;
        state.get_api_context().await
    }

    async fn receive_executed_transaction(&self, transaction: Transaction, sender : Sender<Transaction>) -> Result<(), anyhow::Error> {
       todo!("Receive executed transaction is not yet implemented")
    }

    async fn get_db_reader(&self) -> Result<DbReaderWriter, anyhow::Error> {
        let state = self.state.read().await;
        state.get_db_reader().await
    }

}

#[cfg(test)]
pub mod test {

    use super::*;
    use aptos_storage_interface::state_view::DbStateViewAtVersion;
    use rand::SeedableRng;
    use aptos_types::transaction::{Transaction, ModuleBundle, TransactionPayload};
    use aptos_sdk::{
        transaction_builder::TransactionFactory,
        types::{AccountKey, LocalAccount},
    };
    use aptos_types::{
        account_config::aptos_test_root_address,
        account_view::AccountView,
        block_metadata::BlockMetadata,
        chain_id::ChainId,
        event::EventKey,
        test_helpers::transaction_test_helpers::{block, BLOCK_GAS_LIMIT},
        transaction::{
            Transaction::UserTransaction, TransactionListWithProof, TransactionWithProof,
            WriteSetPayload,
        },
        trusted_state::{TrustedState, TrustedStateChange},
        waypoint::Waypoint,
    };
    use aptos_state_view::account_with_state_view::{AccountWithStateView, AsAccountWithStateView};


    #[tokio::test]
    async fn test_executor() -> Result<(), anyhow::Error> {

        let dir = tempfile::tempdir()?;
        let executor = Executor::new();
        let mut config = NodeConfig::default();
        config.set_data_dir(dir.path().to_path_buf());
        executor.init(config).await?;

        // seed 
        let seed = [3u8; 32];
        let mut rng = ::rand::rngs::StdRng::from_seed(seed);

        // get validator_signer from aptosvm
        let signer = ValidatorSigner::from_int(0);
        // core resources account
        let mut core_resources_account: LocalAccount = LocalAccount::new(
            aptos_test_root_address(),
            AccountKey::from_private_key(aptos_vm_genesis::GENESIS_KEYPAIR.0.clone()),
            0,
        );

        // transaction factory
        let tx_factory = TransactionFactory::new(ChainId::test());

        // accounts
        let mut account1 = LocalAccount::generate(&mut rng);
        let account1_address = account1.address();
        let create1_tx = core_resources_account
            .sign_with_transaction_builder(tx_factory.create_user_account(account1.public_key()));
        let create1_txn = Transaction::UserTransaction(create1_tx);

        // execute the block
        let unix_now = chrono::Utc::now().timestamp_millis() as u64;
        let block = Block {
            ts : unix_now,
            id : HashValue::random(),
            transactions : vec![create1_txn],
        };
        executor.execute_block(block).await?;

        let db = executor.get_db_reader().await?;
        let version = db.reader.get_latest_version()?;
        let state_view = db.reader.state_view_at_version(Some(version))?;
        let account1_state_view = state_view.as_account_with_state_view(&account1_address);
        let account_resource = account1_state_view.get_account_address()?;
        assert!(account_resource.is_some());
        
        Ok(())

    }

}