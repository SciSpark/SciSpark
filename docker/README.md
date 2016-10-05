<h1>Docker</h1>

Docker is an open-source project that automates the deployment of Linux applications inside software containers.
Learn more about docker containers [here](https://www.docker.com/).

<h2> Requirements </h2>

Instal Docker following the guidelines for your system [here](https://docs.docker.com/engine/installation/)

<h2> SciSpark Dev Docker </h2> 
To run the SciSpark docker follow the below steps: 

* Make sure you have docker installed
* Now, ```$ git clone https://github.com/SciSpark/SciSpark.git```
* run ```$ cd SciSpark/docker/```
* If you want to build the docker container from scratch run 

```$ docker build -t scispark/scispark-dev-docker -f Dockerfile .```

else you can pull and existing image from docker hub by running 

```$ docker pull scispark/scispark-dev-docker```

* After the build completed successfully, run the docker by executing this command 

```docker run -it scispark/scispark-dev-docker /bin/bash``` 

or if you have pulled from docker hub run 
   
```docker run -it scispark/scispark-dev-docker /bin/bash``` 
* The above command will open up a shell with root access in the scispark docker

<h2> Development </h2>
To have to ability to develop on your host machine and be able to compile and run those changes in the docker environment, please use the following run command in place of the above:

``` docker run -it -v <absolute path of SciSpark dir>:/root/SciSpark scispark/scispark-dev-docker /bin/bash```

The -v option mounts the SciSpark directory on the users host machine to the one in the docker. 

Once you are done with making changes to the code, run the docker, run ```sbt clean assembly``` and the new jar will be created in the target directory. 
