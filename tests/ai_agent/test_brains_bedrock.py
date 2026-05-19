"""Contract tests for BedrockBrain against a fake converse client."""

from __future__ import annotations

from ai_agent.analytics_tools import build_default_registry
from ai_agent.brains import BedrockBrain, build_tool_config
from ai_agent.reasoning import AgentState, FinalAnswer, ToolCall

_TOOLS = build_default_registry().all()


def test_build_tool_config_shape():
    config = build_tool_config(_TOOLS)
    assert "tools" in config
    names = {t["toolSpec"]["name"] for t in config["tools"]}
    assert "top_content" in names
    spec = config["tools"][0]["toolSpec"]
    assert "inputSchema" in spec and "json" in spec["inputSchema"]


class FakeBedrock:
    """Minimal stand-in for a boto3 bedrock-runtime client."""

    def __init__(self, response):
        self._response = response
        self.last_kwargs = None

    def converse(self, **kwargs):
        self.last_kwargs = kwargs
        return self._response


def test_bedrock_brain_parses_tool_use_block():
    response = {"output": {"message": {"content": [
        {"text": "Need the top shows."},
        {"toolUse": {"name": "top_content", "input": {"limit": 5}, "toolUseId": "t1"}},
    ]}}, "stopReason": "tool_use"}
    brain = BedrockBrain(client=FakeBedrock(response), model_id="m")
    action = brain.next_action(AgentState(conversation_id="c", user_query="top shows"), _TOOLS)
    assert isinstance(action, ToolCall)
    assert action.tool_name == "top_content"
    assert action.args == {"limit": 5}
    assert action.thought == "Need the top shows."


def test_bedrock_brain_parses_final_answer():
    response = {"output": {"message": {"content": [
        {"text": "Sports is the most-watched genre."},
    ]}}, "stopReason": "end_turn"}
    brain = BedrockBrain(client=FakeBedrock(response), model_id="m")
    action = brain.next_action(AgentState(conversation_id="c", user_query="q"), _TOOLS)
    assert isinstance(action, FinalAnswer)
    assert "Sports" in action.text


def test_bedrock_brain_sends_tool_config():
    response = {"output": {"message": {"content": [{"text": "done"}]}},
                "stopReason": "end_turn"}
    fake = FakeBedrock(response)
    BedrockBrain(client=fake, model_id="m").next_action(
        AgentState(conversation_id="c", user_query="q"), _TOOLS)
    assert "toolConfig" in fake.last_kwargs
