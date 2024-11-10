from enum import Enum

class Operation(Enum):
    BEGIN = 0
    WRITE = 1
    COMMIT = 2

    def message(self) -> str:
        if self == Operation.BEGIN:
            return 'BEGIN'
        elif self == Operation.WRITE:
            return 'WRITE'
        elif self == Operation.COMMIT:
            return 'COMMIT'
        else:
            raise Exception("Unknown action.")
    
class RecoveryOperation(Enum):
    REDO = 0
    ABORT = 1