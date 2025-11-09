use super::index::IndexResult;
use crate::core::index::spawn_index_directory_tasks;
use crate::core::index::IndexArgs;
use crate::core::index::IndexDirProgress;
use std::collections::HashMap;
use std::process::Output;

/// The task manager starts tasks and monitors their progress.
/// 
/// Starting a task using the `start_*` functions will give you a task id.
/// Through the task id you can:
///  - abort the task, 
///  - query the progress,
/// 
/// etc.
/// 
/// Stuff like Json-rpc only supports synchronous calls. So this module 
/// was introduced to hide the fancy asnyc stuff and expose 
/// a synchronous, poll-based API to the outside world.
pub struct TaskManager {
    map: HashMap<TaskId, TaskBlock>,
}

struct TaskBlock {
    progress_percentage: Option<f32>,
    progress_fraction: Option<(usize, usize)>,
    aborter: Box<dyn FnOnce() -> ()>,
}

type TaskId = usize;
impl TaskManager {
    fn new() -> Self {
        TaskManager {
            map: HashMap::new(),
        }
    }
    async fn start_index_directory(&self, args: IndexArgs) -> TaskId {
        let (tx, mut rx) = tokio::sync::mpsc::channel(args.channel_capacity);
        let aborter = spawn_index_directory_tasks(args, tx).await;
        let task_block = TaskBlock {
            progress_percentage: None,
            progress_fraction: None,
            aborter: Box::new(aborter),
        };

        // spawn another coroutine to monitor progress
        tokio::spawn(async move {});
        todo!()
    }
}
