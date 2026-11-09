""" A HealthContext object is created at application startup and explicitly passed (injected) into each coroutine or
class that needs it. No global state or singleton. The NATS handler holds a reference to the same HealthContext instance.

Pros:

No global state — most testable and maintainable approach
Explicit dependencies make the code easier to reason about
Scales well as the application grows in complexity
Best alignment with dependency injection principles
Cons:

Requires passing the context object through the call chain, which can feel verbose
Slightly more upfront design effort
"""
import json
from datetime import datetime
from datetime import timezone
from enum import Enum

import pytz
from pydantic import BaseModel


class HealthColor(str, Enum):
    GREEN = "green"
    YELLOW = "yellow"
    RED = "red"

class HealthEvent(BaseModel):
    timestamp: str
    color: HealthColor
    error_type: str
    message: str

    def __init__(self, timestamp, color, error_type, message):
        super().__init__()
        self.timestamp = timestamp
        self.color = color
        self.error_type = error_type
        self.message = message

class HealthData(BaseModel):
    component: str = ""  # not sure why this initialization is needed but Pydantic wants it
    subsystem: str = ""  # not sure why this initialization is needed but Pydantic wants it
    color: HealthColor = HealthColor.GREEN
    events: list[HealthEvent] = []


class HealthContext(HealthData):
    last_reset: str = ""  # not sure why this initialization is needed but Pydantic wants it

    def __init__(self, subsystem, component):
        super().__init__()
        self.subsystem = subsystem
        self.component = component

    def report_error(
        self,
        color: HealthColor,
        error_type: str,
        message: str,
    ):
        if color == HealthColor.RED or (
            color == HealthColor.YELLOW and self.color == HealthColor.GREEN
        ):
            self.color = color

        self.events.append(
            HealthEvent(
                timestamp=datetime.now(tz=pytz.timezone('America/Denver')).isoformat(),
                color=color,
                error_type=error_type,
                message=message,
            )
        )

    def publish_and_reset(self) -> bytearray:
        payload = {
            "subsystem": self.subsystem,
            "component": self.component,
            "status": self.color.value,
            "last_reset": self.last_reset,
            "reported_at": datetime.now(timezone.utc).isoformat(),
            "error_count": len(self.events),
            "events": [
                {
                    "timestamp": e.timestamp,
                    "severity": e.color.value,
                    "error_type": e.error_type,
                    "message": e.message,
                }
                for e in self.events  # looks like this creates a list for the payload dict
            ],
        }
        # convert the payload to a bytearray for the NATS response message
        # using encode() + dumps() to convert to bytes
        # res_bytes = json.dumps(test_dict).encode('utf-8')
        # using decode() + loads() to convert to dictionary
        # res_dict = json.loads(res_bytes.decode('utf-8'))

        json_str = json.dumps(payload)
        response_byte_array = bytearray(json_str, 'utf-8')
        # reset the health status state
        self._reset()

        return response_byte_array

    def _reset(self):
        self.color = HealthColor.GREEN
        self.events = []
        self.last_reset = datetime.now(timezone.utc).isoformat()
