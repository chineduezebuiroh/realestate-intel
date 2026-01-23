# forecast/contracts/errors.py
class ContractError(RuntimeError):
    """Base error for forecast contract violations."""


class MissingRequiredInput(ContractError):
    pass


class KeyMismatch(ContractError):
    pass
