import fsspec


class FileObject:
    """
    File object
    """

    def __init__(
            self,
            full_path: str
    ):
        self.full_path: str = full_path
        self.files: list = fsspec.open_files(self.full_path)

    @property
    def connectable(self) -> bool:
        return True
