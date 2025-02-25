from abc import ABCMeta, abstractmethod

from common_tool.errno import Error

Todo = Error(1001, "todo_not_init")
Ing = Error(1000, "ing_not_todo")
Success = Error(1002, "success_not_ing")
Fail = Error(1003, "fail_not_ing")
Done = Error(1004, "done_not_success_fail")


class TaskStatus(str):
    def is_success(self) -> bool:
        return self == SUCCESS


INIT = TaskStatus("init")
TODO = TaskStatus("todo")
ING = TaskStatus("ing")
SUCCESS = TaskStatus("success")
FAIL = TaskStatus("fail")
DONE = TaskStatus("done")


class Task(metaclass=ABCMeta):
    @abstractmethod
    def id(self) -> str:
        pass

    @abstractmethod
    def status(self) -> TaskStatus:
        pass

    def is_init(self) -> bool:
        return self.status() == INIT

    def is_todo(self) -> bool:
        return self.status() == TODO

    def is_ing(self) -> bool:
        return self.status() == ING

    def is_success(self) -> bool:
        return self.status() == SUCCESS

    def is_fail(self) -> bool:
        return self.status() == FAIL

    def is_done(self) -> bool:
        return self.status() == DONE

    @abstractmethod
    def mark_todo(self, force: bool = False) -> Error:
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

    @abstractmethod
    def mark_done(self) -> Error:
        pass

    @abstractmethod
    def clean(self) -> Error:
        pass

    def todo(self, force=False) -> Error:
        if (force and self.is_ing()) or self.is_init():
            return self.mark_todo(force)
        return Todo

    def ing(self) -> Error:
        if not self.is_todo():
            return Ing
        return self.mark_ing()

    def success(self) -> Error:
        if not self.is_ing():
            return Success
        return self.mark_success()

    def fail(self) -> Error:
        if not self.is_ing():
            return Fail
        return self.mark_fail()

    def done(self) -> Error:
        if not self.is_success() and not self.is_fail():
            return Done
        return self.mark_done()
