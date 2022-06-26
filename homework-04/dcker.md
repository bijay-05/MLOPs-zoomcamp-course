$ docker run -it --rm deploy_hw


rm above tells the docker daemon to clean up the container
and remove the file system after the container exits. This helps you save disk space
after running shot-lived containers like this one.

in docker file

RUN pipenv install --system --deploy

it is used to install dependencies in the system rather than creating virtual envs