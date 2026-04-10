import abc
from typing import Any, Union


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self.data_store: list[tuple[int, str]] = []
        self.total_processed: int = 0

    @abc.abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self.data_store:
            raise ValueError("No data available in the processor.")
        return self.data_store.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, bool):
            return False
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            return all(isinstance(
                x, (int, float)) and not isinstance(x, bool) for x in data
            )
        return False

    def ingest(self, data: Union[int, float, list[Union[int, float]]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")

        if isinstance(data, list):
            for item in data:
                self.data_store.append((self.total_processed, str(item)))
                self.total_processed += 1
        else:
            self.data_store.append((self.total_processed, str(data)))
            self.total_processed += 1


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(x, str) for x in data)
        return False

    def ingest(self, data: Union[str, list[str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")

        if isinstance(data, list):
            for item in data:
                self.data_store.append((self.total_processed, item))
                self.total_processed += 1
        else:
            self.data_store.append((self.total_processed, data))
            self.total_processed += 1


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        def is_valid_dict(d: Any) -> bool:
            return isinstance(d, dict) and all(
                isinstance(k, str) and isinstance(v, str) for k, v in d.items()
                )

        if is_valid_dict(data):
            return True
        if isinstance(data, list):
            return all(is_valid_dict(x) for x in data)
        return False

    def ingest(
        self, data: Union[dict[str, str], list[dict[str, str]]]
    ) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        if isinstance(data, dict):
            data = [data]

        for item in data:
            level = item.get('log_level', 'UNKNOWN')
            msg = item.get('log_message', '')
            formatted = (
                f"{level}: {msg}"
                if 'log_level' in item and 'log_message' in item
                else str(item)
            )
            self.data_store.append((self.total_processed, formatted))
            self.total_processed += 1


if __name__ == "__main__":
    print("=== Code Nexus Data Processor ===\n")

    np = NumericProcessor()
    print("Testing Numeric Processor...")
    print(f"Trying to validate input '42': {np.validate('42')}")
    print(f"Trying to validate input 'Hello': {np.validate('Hello')}")
    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        np.ingest('foo')
    except Exception as e:
        print(f"Got exception: {e}")

    print("Processing data: [1, 2, 3, 4, 5]")
    np.ingest([1, 2, 3, 4, 5])
    print("Extracting 3 values...")
    for _ in range(3):
        rank, val = np.output()
        print(f"Numeric value {rank}: {val}")

    print()
    tp = TextProcessor()
    print("Testing Text Processor...")
    print(f"Trying to validate input '42': {tp.validate(42)}")
    print("Processing data: ['Hello', 'Nexus', 'World']")
    tp.ingest(['Hello', 'Nexus', 'World'])
    print("Extracting 1 value...")
    rank, val = tp.output()
    print(f"Text value {rank}: {val}")

    print()
    lp = LogProcessor()
    print("Testing Log Processor...")
    print(f"Trying to validate input 'Hello': {lp.validate('Hello')}")
    log_data = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!!'}
    ]
    print(f"Processing data: {log_data}")
    lp.ingest(log_data)
    print("Extracting 2 values...")
    for _ in range(2):
        rank, val = lp.output()
        print(f"Log entry {rank}: {val}")
