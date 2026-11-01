from dataclasses import dataclass

from temporalio import activity


@dataclass
class GreetingInput:
    name: str
    language: str = "en"


@activity.defn
async def compose_greeting(input: GreetingInput) -> str:
    activity.logger.info("Composing a greeting for %s", input.name)
    words = {"en": "Hello", "es": "Hola", "hi": "Namaste"}
    return f"{words.get(input.language, 'Hello')}, {input.name}!"
