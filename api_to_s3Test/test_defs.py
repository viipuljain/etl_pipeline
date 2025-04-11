from api_to_s3.__init__ import defs


def test_def_can_load():
    assert defs.get_job_def("pipeline_job")
