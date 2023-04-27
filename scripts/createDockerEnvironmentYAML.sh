# purpose of this is to create a clean environment and build a yaml file of the environment for docker
# this is useful when extra packages you no longer need were left on your development environment

source ~/miniconda3/bin/activate
# conda init bash  # don't actually think this is necessary you just need to source the activate command

# Remove environment if it exists
echo "Deleting py37_clean environment if exists"
yes | conda remove --name py37_clean --all

# create new environment
echo "Creating py37_clean environment"
yes | conda create -n py37_clean python=3.7 pip
echo "Activating py37_clean environment"
conda activate py37_clean
echo "Installing pip requirements"
pip install -r ../requirements.txt

# create yaml file
echo "Exporting ../model/environment.yml file"
# conda env export > ../model/environment.yml  # currently this doesn't work the way we need
#  Below creates a dependency file without environment name, microconda (in docker) needs base environment
python conda_env_export.py -o ../model/environment.yml

echo "exiting environment"
conda deactivate

# delete environment
# echo "Deleting py37_clean environment"
# yes | conda remove --name py37_clean --all
