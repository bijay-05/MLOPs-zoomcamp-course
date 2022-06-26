$ docker run -it --rm deploy_hw


rm above tells the docker daemon to clean up the container
and remove the file system after the container exits. This helps you save disk space
after running shot-lived containers like this one.

in docker file

RUN pipenv install --system --deploy

it is used to install dependencies in the system rather than creating virtual envs

ADD codes /root/test

ADD codes/code1.py codes/code2.py /root/test/

when adding multiple source files or directories, there must be a / at 
the end of the destination directory
