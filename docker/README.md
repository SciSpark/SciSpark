<h1>Docker</h1>

Docker is an open-source project that automates the deployment of Linux applications inside software containers.
Learn more about docker containers [here](https://www.docker.com/).

<h2> Requirements </h2>

Instal Docker following the guidelines for your system [here](https://docs.docker.com/engine/installation/)

<h2> SciSpark Docker </h2> 
To run the SciSpark docker follow the below steps: 

* Make sure you have docker installed
* Now, ```$ git clone https://github.com/SciSpark/SciSpark.git```
* run ```$ cd SciSaprk/docker/```
* If you want to build the docker container from scratch run 

```$ docker build -t scispark -f Dockerfile .```

else you can pull and existing image from docker hub by running 

```$ docker pull sujenshah/scispark```

* After the build completed successfully, run the docker by executing this command 

```docker run -it scispark /bin/bash``` 

or if you have pulled from docker hub run 
   
```docker run -it sujenshah/scispark /bin/bash``` 
* The above command will open up a shell with root access in the scispark docker
