from datetime import datetime

LOG_FILE = "rto_log.txt"

class LogMessage:
    """
    Create a standard log message
    """
    def __init__(self, progress: str, id: int, status: str, msg: str, additional_info=None):
        self.progress = progress
        self.id = id
        self.status = status
        self.msg = msg
        self.additional_info = additional_info

    def short_msg(self) -> str:
        """
        :return: Shorten form of the log message
        """
        return f"{self.progress} | {self.id}"

    def log(self, print_short=False, save_to_file=False) -> None:
        if print_short:
            print(self.short_msg())
        else:
            print(self)
        if save_to_file:
            with open(LOG_FILE, "a+") as f:
                f.write(str(self) + "\n")

    def __str__(self):
        str = f"{self.progress} | {self.id} | {datetime.now()} | {self.status} | {self.msg}"
        if self.additional_info is not None:
            str += f" | {self.additional_info}"
        return str