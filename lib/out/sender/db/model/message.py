from dataclasses import dataclass
from datetime import datetime
from typing import List, Sequence


@dataclass
class Message:
    date: str
    origin: str
    rel_name: str
    db_name: str
    data: Sequence[str]
