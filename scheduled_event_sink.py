"""Translate a pinned emitter envelope into the existing scheduled binding.

Only the canonical event identifier carries the frozen scope and UTC slot.
Delivery time, generation and evidence can never choose a command or slot.
"""
from __future__ import annotations

import base64
import json
import math
import re

from scheduled_receiver import run_id


class ScheduledEventSink:
    def __init__(self, receiver, emitter: str, recipient: str):
        if (type(emitter) is not str or
                not re.fullmatch(r'[A-Za-z][A-Za-z0-9_.-]{0,127}', emitter) or
                type(recipient) is not str or not 0 < len(recipient) <= 256):
            raise ValueError('pinned emitter and recipient required')
        receiver.scope.validate()
        self.receiver, self.scope = receiver, receiver.scope
        self.emitter, self.recipient = emitter, recipient

    def delivery_scope(self):
        return {'receiver': 'scheduled-event-v1', 'emitter': self.emitter,
                'recipient': self.recipient, 'binding': self.receiver.delivery_scope()}

    def deliver(self, event: dict) -> None:
        required = {'event_id', 'event', 'emitter', 'item', 'generation',
                    'recipient', 'evidence', 'observed_at'}
        if (type(event) is not dict or not required <= event.keys() or
                event.keys() - required - {'work_kind'}):
            raise ValueError('complete supported emitter envelope required')
        if (event['event'] != 'scheduled' or event['emitter'] != self.emitter or
                event['item'] != self.scope.job or event['recipient'] != self.recipient):
            raise ValueError('event differs from pinned receiver routing')
        generation, observed = event['generation'], event['observed_at']
        if (type(generation) is not int or not 0 <= generation < 2**63 or
                type(observed) not in (int, float) or
                not 0 <= observed <= 253402300799 or not math.isfinite(observed)):
            raise ValueError('invalid emitter metadata')
        evidence = event['evidence']
        if evidence is not None and (type(evidence) is not str or len(evidence) > 8192):
            raise ValueError('bounded textual evidence required')
        if 'work_kind' in event and (type(event['work_kind']) is not str or
                not re.fullmatch(r'[A-Za-z][A-Za-z0-9_-]{0,127}', event['work_kind'])):
            raise ValueError('invalid work kind')
        identity = event['event_id']
        if (type(identity) is not str or len(identity) > 24000 or
                not identity.startswith('scheduled-v1-')):
            raise ValueError('bounded canonical scheduled identifier required')
        try:
            encoded = identity[len('scheduled-v1-'):]
            raw = base64.b64decode(encoded + '=' * (-len(encoded) % 4),
                                   altchars=b'-_', validate=True)
            binding = json.loads(raw)
            if type(binding) is not dict:
                raise ValueError('scheduled binding must be an object')
            slot = binding.get('scheduled_at')
            if type(slot) is not int or not 0 <= slot <= 253402300799:
                raise ValueError('bounded integer UTC slot required')
            # Exact re-encoding refuses a foreign scope, duplicate JSON keys,
            # extra fields, booleans in integer positions, padding and alternate
            # JSON spellings. The observed delivery timestamp is not an anchor.
            if run_id(self.scope, slot) != identity:
                raise ValueError('scheduled identifier differs from pinned scope')
        except (ValueError, UnicodeError, RecursionError) as exc:
            raise ValueError('invalid canonical scheduled identifier') from exc
        del binding['version']
        self.receiver.deliver(binding)

    def close(self):
        self.receiver.close()
