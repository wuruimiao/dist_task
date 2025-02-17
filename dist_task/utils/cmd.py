import subprocess


def run_cmd(cmd: list[str], env=None, timeout=3) -> tuple[str, bool]:
    try:
        out = subprocess.check_output(
            cmd, timeout=timeout, env=env,
            # pass arguments as a list of strings
            # shell=True,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
        )
        out = out.replace("\r\n", "\n")
        print(f"{cmd} output={out}")
        return out, True
    except subprocess.CalledProcessError as e:
        out = e.output
        print(f"{cmd} output={out}")
        return out, False
    except (FileNotFoundError, PermissionError) as e:
        out = f"{e}"
        print(f"{cmd} output={out}")
        return out, False
    except subprocess.TimeoutExpired as e:
        out = f"{e}"
        print(f"{cmd} timeout output={out}")
        return out, False
