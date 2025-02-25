from pathlib import Path
from collections import defaultdict
from common_tool.system import remote_list_dir


def list_dir(is_remote: bool, host: str, user: str, folder: Path) -> tuple[dict[str, set[Path]], dict[str, set[Path]]]:
    """
    远程时，返回的Path不能做is_file/is_dir操作
    :param is_remote:
    :param host:
    :param user:
    :param folder:
    :return: file.stem: {full path}
    """
    files, dirs = defaultdict(set), defaultdict(set)
    if is_remote:
        _filenames, _dir_names = remote_list_dir(host, user, folder)

        _filenames = [folder.joinpath(f) for f in _filenames]
        [files[f.stem].add(f) for f in _filenames]

        _dir_names = [folder.joinpath(f) for f in _dir_names]
        [dirs[f.stem].add(f) for f in _dir_names]
    else:
        for f in folder.iterdir():
            if f.is_file():
                files[f.stem].add(f)
            elif f.is_dir():
                dirs[f.stem].add(f)

    return files, dirs
