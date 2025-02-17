class Error(object):
    def __init__(self, code_int: int, code: str, desc: str = ""):
        self.code_int = code_int
        self.code = code
        self.desc = desc

    @property
    def ok(self):
        return self.code_int == 0

    def __str__(self):
        return f"code={self.code_int} message={self.code}:{self.desc}"

    @property
    def error(self) -> str:
        return self.desc

    def dict(self) -> dict:
        return {"code": self.code_int, "message": self.desc}


OK = Error(0, "", "")
