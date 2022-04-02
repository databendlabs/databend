use common_exception::Result;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_schedule_plan() -> Result<()> {

    // let context = create_env().await?;
    // let scheduler = PlanScheduler::try_create(context)?;
    // let scheduled_tasks = scheduler.reschedule(&PlanNode::Empty(EmptyPlan::create()))?;
    //
    // assert!(scheduled_tasks.get_tasks()?.is_empty());
    // assert_eq!(
    //     scheduled_tasks.get_local_task(),
    //     PlanNode::Empty(EmptyPlan::create())
    // );

    Ok(())
}