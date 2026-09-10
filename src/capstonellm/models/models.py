from pydantic import BaseModel, ConfigDict


class StackOverflowQuestion(BaseModel):
    model_config = ConfigDict(extra="ignore")

    question_id: int
    title: str
    body: str


class StackOverflowAnswer(BaseModel):
    model_config = ConfigDict(extra="ignore")

    answer_id: int
    question_id: int
    score: int
    body: str
    is_accepted: bool = False