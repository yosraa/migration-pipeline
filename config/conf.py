import os


class Config:
    DATABASE_NAME = ''
    TABLE_NAME = ''
    AWS_EXTRA_PARAMETERS: dict[str, str] = {}


class LocalConfig(Config):
    DEBUG = True
    s3_path = "" + os.getenv("PLATFORM", "test")
    BUCKET = "files-" + os.getenv("PLATFORM", "test")


class UnitTestConfig(Config):
    pass


config_by_name = dict(
    local=LocalConfig,
    unit_test=UnitTestConfig
)
