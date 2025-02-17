from abc import ABCMeta, abstractmethod
from dist_task.utils.errno import Error

Ing = Error(1000, "ing_not_todo")


class Task(metaclass=ABCMeta):
    @abstractmethod
    @property
    def id(self) -> str:
        pass

    @abstractmethod
    def is_todo(self) -> bool:
        pass

    @abstractmethod
    def is_ing(self) -> bool:
        pass

    @abstractmethod
    def mark_todo(self) -> Error:
        pass

    @abstractmethod
    def mark_ing(self) -> Error:
        pass

    @abstractmethod
    def mark_success(self) -> Error:
        pass

    @abstractmethod
    def mark_fail(self) -> Error:
        pass

    def ing(self) -> Error:
        if not self.is_todo():
            return Ing
        return self.mark_ing()

    def success(self) -> Error:
        if not self.is_ing():
            print(f"success not ing {self}")
        return self.mark_success()

    def fail(self) -> Error:
        if not self.is_ing():
            print(f"fail not ing {self}")
        return self.mark_fail()
