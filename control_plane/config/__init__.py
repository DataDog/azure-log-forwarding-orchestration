async def main(timer):
    from .task import main as task_main  # type: ignore

    await task_main()
