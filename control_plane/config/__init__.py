# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.
#
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.


async def main(timer):
    from .task import main as task_main  # type: ignore

    await task_main()
