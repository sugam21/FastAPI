conda-create:
	conda env create -f environment.yml
	echo "!!! Activate the environment using `conda activate datascience_env`"

poetry:
	poetry install 
