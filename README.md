# Big data project

⚠ This repo is complete ! Since we have untill the 19th, we will do a last push at the 19th, 23h. However you can start to take a look at the project.
## Introduction
This project aims to analyze and do some machine learning on a sample stack overflow questions data. It contains several parts. The data comes originaly from [the stack exchange archive website](https://archive.org/details/stackexchange) but we explain in the data part how to get exclusively the tables we used (we just took some tables concerning stack overflow and put it at your disposal on a open drive).
- One part (\Data folder) concerns the data retrieving and loading. (Be sure to look at the readme on this folder for more information)
- A second part concerns the data exploration of this sample using pyspark
- A third part concerns clustering of the questions with Latent Dirichlet Allocation (LDA) using Spark Ml
- A final part concerns multi class classification of the tags of a question using Keras, SparkMl and SparkMllib

## Installation 
This project has been mounted with a Docker image to be able to run easaly each notebooks which are based on differents frameworks. 

### Getting the Docker image
To use the image you can either mount it from the Docker file by using : 
```{shell}
path/to/repo> docker build -t loicbausor/simaposor .
```
Or download it from the cloud by using : 
```{shell}
docker pull loicbausor/simaposor
```
### Running the image 
To run the image and the notebooks inside the container use this command : 
```{shell}
> docker run -d -p 8888:8888 -p 4040:4040 -p 4041:4041 -v path/to/repo:/home/loic -e JUPYTER_TOKEN=your_custom_password -e JUPYTER_RUNTIME_DIR=/tmp --cpus=3 loicbausor/simaposor
```
* The first port argument is the port argument of the jupyter notebook, the second and the third are the one for spark dashboards. Be sure that those ports are available on your machine.
* The volume argument connects the repo directory to the work directory of the notebooks (make sure to change it to the repo folder).
* The jupyter token is the password that would be asked to enter jupyter. 

We use 3 cpus but feel free to change it. Please, you need to allocate at least 8G of RAM to be sure all the project run correctly.

### Open jupyter notebook 
1. Open your internet navigator
2. Search for the address : http://localhost:8888/
3. Enter the password you put as JUPYTER_TOKEN env variable (your_custom_password in the exemple)

You can now open and get through all the notebooks.

## Project summary
### Getting the data 
The /Data folder contains one Notebook named `DataLoader.ipynb`. Its goal is to sample the stack overflow data which is too heavy to be treated. 

⚠ **This notebook is not meant to be runned**. It can be runned if you follow the instructions in it but it will be **really long**. It is just here to show how we sampled the data.
**All the other notebooks are based on the produced sampled data which will be automaticaly dowloaded by custom functions when needed**. This data sample is can be also be mannually downloaded [here](https://drive.google.com/drive/folders/1ddsBX4I4hZ8pordSKf5cHRaVBnNVOcKk) if needed.

The main problematics of this sampling step were in term of computation time and overload of memory. As a result, we never used at once the full sample we have done. 

### Some First Insights
The /Notebooks folder contains a Notebook named `Some first insights.ipynb`.

Its goal is to present the sampled stackoverflow data we have, do plots about it etc. It is the fun/playground part of the project where we are scrapping information on the content of our data.

### Features analisys
The /Notebooks folder also contains the Notebook named `Text features analysis.ipynb`.

In this part we try to detect some patterns in the text of the posts:  we use the Latent Dirichlet Allocation in order to detect main topics inside our corpus of questions. You can find [here](https://www.mygreatlearning.com/blog/understanding-latent-dirichlet-allocation/) more info on how LDA works in essence. 

We also compare those topics with the tags associated to the questions.

### ML
This part can be found in the /Notebooks folder and the notebook named `Text features analysis.ipynb`.

For this last part we wanted to do some Multiclass classification of the tags based on the text of each questions. The instruction was to use :

 - pyspark.ml library
 - pyspark.mllib library
 - A third party library in a spark pipeline

We have been disapointed to learn that pyspark ml and mllib don't have multi label classification implementation for its algorithm. Indeed those one can do multi class classification but not multi label.
A way to go by this issue would have been to use a string indexer to transform our labels. 
*Example*: if we have three classes  {𝐴,𝐵,𝐶} , the string indexer creates new classes {𝐴𝐵, 𝐵𝐶, 𝐴𝐶, 𝐴𝐵𝐶}, so the model just becomes multiclass problem. There are several issues with this method :

- **Dimensionnality** : we have a lot more than three tags in the sample. It would have been tramendous computations for really poor results
- A enormous **loss in information**, we would a have lost a part of the label correlation.

After some expensive computations and non concluent tests we decided that we will do only multiclass classification on the 10 most used tags.
This notebooks presents the results on the test set. 