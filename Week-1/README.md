### Install Make
- Install chocolaty if you dont have from [install-chocolaty-from-here](https://chocolatey.org/install).
- Install make using `choco install make`.
>> Note: Make sure you have administrative rights to terminal before doing anything.

### Create conda environment
- Type `make conda-install` in your vs-code terminal.
- Make sure you activate your environment using `conda activate <env>`.

### Install poetry and dependencies
- `make poetry`
Explaination
- `poetry install`. This will install the dependencies from pyproject.toml using poetry.lock file.

## Run the Server
```bash
make run-server
```