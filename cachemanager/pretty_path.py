import os
from pathlib import Path
# From https://stackoverflow.com/a/74301061/1261153


def shorten_path(input_path: Path, max_length: int) -> str:
    input_ = str(input_path)
    if len(input_) < max_length:
        return input_  # no need to shorten

    shortened_path = "..."  # add middle item
    paths_to_choose_from = input_.split(
        os.sep
    )  # split by your custom OS separator. "/" for linux, "\" for windows.

    add_last_path = True
    while len(shortened_path) < max_length:
        if len(paths_to_choose_from) == 0:
            return shortened_path

        if add_last_path:
            shortened_path = shortened_path.replace(
                "...", f"...{os.sep}{paths_to_choose_from[-1]}"
            )
            del paths_to_choose_from[-1]  # delete elem used
            add_last_path = False
        else:
            shortened_path = shortened_path.replace(
                "...", f"{paths_to_choose_from[0]}{os.sep}..."
            )
            del paths_to_choose_from[0]  # delete elem used
            add_last_path = True
    return shortened_path
