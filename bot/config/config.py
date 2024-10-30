from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_ignore_empty=True)

    API_ID: int
    API_HASH: str

    USE_RANDOM_DELAY_IN_RUN: bool = False
    RANDOM_DELAY_IN_RUN: list[int] = [5, 9930]
   
    REF_ID: str = "ref-xAF7MD7TLUUo6sAwysTboE"

    SLEEP_TIME: list[int] = [32000, 60000]
    USE_PROXY: bool = False


settings = Settings()


