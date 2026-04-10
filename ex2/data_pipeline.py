import abc
from typing import Any, Union, Protocol


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
            return all(
                isinstance(x, (int, float)) and not
                isinstance(x, bool) for x in data
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


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        if not data:
            return
        output_str = ", ".join(item[1] for item in data)
        print(output_str)


class JSONExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        if not data:
            return
        items = []
        for rank, val in data:
            safe_val = val.replace('"', '\\"')
            items.append(f'"item_{rank}": "{safe_val}"')
        json_str = "{" + ", ".join(items) + "}"
        print(json_str)


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for item in stream:
            processed = False
            for proc in self.processors:
                if proc.validate(item):
                    proc.ingest(item)
                    processed = True
                    break
            if not processed:
                print(
                    f"DataStream error Can't process element in stream: {item}"
                )

    def print_processors_stats(self) -> None:
        print("\n== DataStream statistics ==")
        if not self.processors:
            print("No processor found, no data\n")
            return
        for proc in self.processors:
            name = proc.__class__.__name__.replace("Processor", " Processor")
            print(
                f"{name}: total {proc.total_processed} items processed, "
                f"remaining {len(proc.data_store)} on processor"
            )
        print()

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self.processors:
            extracted_data = []
            for _ in range(nb):
                try:
                    extracted_data.append(proc.output())
                except ValueError:
                    break
            if extracted_data:
                plugin.process_output(extracted_data)


if __name__ == "__main__":
    print("=== Code Nexus Data Pipeline ===\n")
    print("Initialize Data Stream...\n")
    stream = DataStream()
    stream.print_processors_stats()

    np = NumericProcessor()
    tp = TextProcessor()
    lp = LogProcessor()

    print("Registering Processors\n")
    stream.register_processor(np)
    stream.register_processor(tp)
    stream.register_processor(lp)

    data_batch_1 = [
        'Hello world',
        [3.14, 1, 2.71],
        [{
            'log_level': 'WARNING',
            'log_message': 'Telnet access! Use ssh instead'
            },
         {'log_level': 'INFO', 'log_message': 'User wil is connected'}],
        42,
        ['Hi', 'five']
    ]
    print(f"Send first batch of data on stream: {data_batch_1}\n")
    stream.process_stream(data_batch_1)
    stream.print_processors_stats()

    print("Send 3 processed data from each processor to a CSV plugin:")
    csv_plugin = CSVExportPlugin()
    stream.output_pipeline(3, csv_plugin)
    stream.print_processors_stats()

    data_batch_2 = [
        21,
        ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        [{'log_level': 'ERROR', 'log_message': '500 server crash'},
         {
            'log_level': 'NOTICE',
            'log_message': 'Certificate expires in 10 days'
            }],
        [32, 42, 64, 84, 128, 168],
        'World hello'
    ]
    print(f"Send another batch of data: {data_batch_2}\n")
    stream.process_stream(data_batch_2)
    stream.print_processors_stats()

    print("Send 5 processed data from each processor to a JSON plugin:")
    json_plugin = JSONExportPlugin()
    stream.output_pipeline(5, json_plugin)
    stream.print_processors_stats()
