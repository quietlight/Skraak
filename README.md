# Skraak

This package serves [skraak.kiwi](https://skraak.kiwi), recognising and classifying Haast Kiwi calls from Pomona Island, NZ. 

Most of the data is recorded using Open Acoustics AudioMoth's or μMoth's at 16000 Hz.

Skraak arose from my use of AviaNZ (Python), and then Opensounscape (Python). The Skraak call recognition pipeline is mostly modeled on Opensoundscape.

Skraak can be useful to many conservation projects in NZ for monitoring bird call frequency. 

Since Skraak works on colour images, it can potentially be used for identifying predators in trail camera images too.

__Skraak is intended to be simple to use for simple people like me.__

It is a good idea to use an Nvidia GPU. Everything should work fine on CPU, just slow. AMD GPU's are not supported but should be easy for you to get working.

```
1. Take some WAV's organised into a file structure LOCATION/TRIP_DATE/WAV_FILES 
2. and labels saved in a csv in the form:
    * file_path, label, "[start, end]" (where start and end are in seconds from the start of the wav file)
    * at least 2 label classes are required, for example Kiwi, NotKiwi

3. Generate a primary dataset of spectrogram images with the file structure:
    DATASET/LABEL/PNG's (png files must be 224X224 px square, RGB)

4. Train a Resnet 18 model, either pretrained on Imagenet, or preferably the pretrained Skraak Kiwi model which is currently trained on 7_400_000 images from both night and daytime audio from Pomona Island, Haast and Secretary Island.

5. Run inference on raw data using a trained model
6. Generate audio clips and spectrogram images of all calls found.
7. Sort calls into subclasses (say: Duet, Female, Male, Something, Nothing) manually or using a model combined with manual classification

8. Store data from calls and file metadata in a DuckDB database for statistical analysis using SQL, DataFrames, Plots.

9. Repeat,iterating on your models as you accumulate more data. It's hard until it gets easy.
```

Managing datasets is like gardening, it takes some weeding and a _lot_ of compost (aka data) to get a good model growing. 

Currently grayscale RGB spectrogram images are used. Colour spectrogram images add no new information, yet strangely train fastest. During inference, when looking for calls in raw data, many millions of images need to be generated on the fly, applying the colour layer is very time intensive, so I compromise on (grayscale) RGB images for use with pretrained models for fast-ish training, and fast-ish inference using grayscale (RGB) images.

Julia is great for machine learning because it is realtively simple to get a GPU working. It does have disadvantages at GPT-4 scale, but for this kind of work it is excelent. Julia shines with any scientific computing task.

Skraak is much more stable than Opensoundscape/Python when training on many millions of images. Avianz/Python has some good ideas, such as JSON labels.



