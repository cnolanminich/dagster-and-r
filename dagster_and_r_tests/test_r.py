from dagster_and_r.docker_and_r import my_docker_and_r_job

def test_my_docker_and_r_job():
    """
    Test calling R job from docker.
    """
    res = my_docker_and_r_job.execute_in_process()
    assert res.success
