import csv


def load_list(path):
    with open(path) as csv_file:
        l = []
        csv_reader = csv.reader(csv_file, delimiter=',')
        line = 0
        for row in csv_reader:
            line += 1
            if line == 1:
                continue
            l.append(row[0])
        return l


keyword_list = load_list("../clean_data/depr_word_list.csv")
tags100_raw = load_list("../clean_data/tags_freq.csv")
tags_apr_raw = load_list("../clean_data/tags_apriori.csv")

tags_apr = [
    'deep-learning',
    'tensorflow',
    'keras',
    'machine-learning',
    'neural-network',
    'conv-neural-network',
    'computer-vision',
    'pytorch',
    'lstm',
    'tensorflow2.0',
    'caffe',
    'numpy',
    'classification',
    'recurrent-neural-network',
    'nlp',
    'loss-function',
    'tf.keras',
    'image-processing',
    'object-detection',
    'scikit-learn',
    'autoencoder',
    'artificial-intelligence',
    'pycaffe',
    'keras-layer',
    'reinforcement-learning',
    'google-colaboratory',
    'theano',
    'opencv',
    'word2vec',
    'pandas',
    'vgg-net',
    'tensorboard',
    'gradient-descent',
    'bert-language-model',
    'backpropagation',
    'regression',
    'huggingface-transformers',
    'training-data',
    'mnist',
    'tensor',
    'time-series',
    'torch',
    'data-science',
    'resnet',
    'yolo',
    'convolution',
    'transfer-learning',
    'generative-adversarial-network',
    'gpu',
    'image-segmentation'
]


tags_huge = [
    'deep-learning',
    'tensorflow',  # lib
    'keras',  # lib
    'machine-learning',
    'neural-network',
    'pytorch',  # lib
    'conv-neural-network',
    'computer-vision',
    'lstm',
    'caffe',  # lib
    'nlp',
    'image-processing',
    'recurrent-neural-network',
    'numpy',  # lib
    'object-detection',
    'artificial-intelligence',
    'tensorflow2.0',  # lib
    'classification',
    'autoencoder',
    'opencv',  # lib
    'reinforcement-learning',
    'theano',  # lib
    'loss-function',
    'image-segmentation',
    'google-colaboratory',  # lib
    'gpu',
    'keras-layer',  # lib
    'scikit-learn',  # lib
    'generative-adversarial-network',
    'transfer-learning',
    'tf.keras',  # lib
    'convolution',
    'yolo',
    'resnet',
    'data-science',
    'torch',  # lib
    'pycaffe',  # lib
    'time-series',
    'tensor',
    'mnist',
    'training-data',
    'regression',
    'huggingface-transformers',  # lib
    'backpropagation',
    'bert-language-model',
    'gradient-descent',
    'tensorboard',  # lib
    'vgg-net',
    'pandas',  # lib
    'word2vec',
    'semantic-segmentation',
    'mxnet',  # lib
    'rnn',
    'batch-normalization',
    'tensorflow-datasets',  # lib
    'jupyter-notebook',
    'word-embedding',
    'attention-model',
    'fast-ai',  # lib
    'image-classification',
    'deeplearning4j',  # lib
    'tensorflow-lite',  # lib
    'pre-trained-model',
    'text-classification',
    'transformer-model',
    'multilabel-classification',
    'q-learning',
    'image-recognition',
    'data-augmentation',
    'h2o',  # lib
    'activation-function',
    'object-detection-api',
    'softmax',
    'convolutional-neural-network',
    'loss',
    'feature-extraction',
    'multiclass-classification',
    'nvidia',
    'tflearn',  # lib
    'anaconda',  # lib
    'face-recognition',
    'openai-gym',  # lib
    'hyperparameters',
    'cntk',  # lib
    'ocr',
    'torchvision',  # lib
    'onnx',  # lib
]
tags_medium = tags_huge[:50]
tags_small = tags_huge[:20]


tag_id_dict = {}
for i, tag in enumerate(tags100_raw):
    tag_id_dict[tag] = i
    tag_id_dict[i] = tag

tags_libs = [
    'tensorflow',
    'keras',
    'pytorch',
    'caffe',
    'numpy',
    'tensorflow2.0',
    'opencv',
    'theano',
    'google-colaboratory',
    'keras-layer',
    'scikit-learn',
    'tf.keras',
    'torch',
    'pycaffe',
    'huggingface-transformers',
    'tensorboard',
    'pandas',
    'mxnet',
    'tensorflow-datasets',
    'jupyter-notebook',
    'fast-ai',
    'deeplearning4j',
    'tensorflow-lite',
    'h2o',
    'tflearn',
    'anaconda',
    'openai-gym',
    'cntk',
    'torchvision',
    'onnx',
]

tags_libs_clean = [
    'tensorflow',
    'keras',
    'pytorch',
    'caffe',
    'numpy',
    'tensorflow2.0',
    'opencv',
    'theano',
    'keras-layer',
    'scikit-learn',
    'tf.keras',
    'torch',
    'pycaffe',
    'huggingface-transformers',
    'tensorboard',
    'pandas',
    'mxnet',
    'tensorflow-datasets',
    'fast-ai',
    'deeplearning4j',
    'tensorflow-lite',
    'h2o',
    'tflearn',
    'anaconda',
    'openai-gym',
    'cntk',
    'torchvision',
    'onnx',
]

equivalent_tags_libs = {
    'tensorflow': [
        'tensorflow2.0',
        'tensorboard',
        'tensorflow-datasets',
        'tensorflow-lite',
        'tflearn',],
    'pytorch': ['torchvision'],
    'keras': ['tf.keras', 'keras-layer'],
    'caffe': ['pycaffe'],
}

tags_libs_apr = [
    'tensorflow',
    'keras',
    'pytorch',
    'caffe',
    'numpy',
    'tensorflow2.0',
    'opencv',
    'theano',
    'google-colaboratory',
    'keras-layer',
    'scikit-learn',
    'tf.keras',
    'torch',
    'pycaffe',
    'huggingface-transformers',
    'tensorboard',
    'pandas'
]


cols_base = [

    "Id",
    "AcceptedAnswerId",
    "ParentId",
    "PostTypeId",
    "CreationDate",
    "LastEditDate",
    "DeletionDate",
    "LastActivityDate",
    "OwnerDisplayName",
    "LastEditorDisplayName",
    "Title",
    "Body",
    "Tags",
    "Score",
    "ViewCount",
    "AnswerCount",
    "CommentCount",
    "FavoriteCount"
]
cols_out = [
    "Id",
    # "AcceptedAnswerId",
    # "ParentId",
    # "PostTypeId",
    "CreationDate",
    # "LastActivityDate",
    "ActivityMonths",
    # "IsDeleted",
    "Tags",
    "Content",
    # "AnswerContent",
    "AcceptedAnswerRelated",
    "RelAnswerCount",
    "TotAnswerCount",
    "Score",
    "ViewCount",
    # "CommentCount",
    # "FavoriteCount"
]


def wrap_tags(tags):
    return list(map(lambda x: '<' + x + '>', tags))


def find_unique(tags1, tags2):
    unique = []
    for tag in tags1:
        if not tag in tags2:
            unique.append(tag)
    return unique


def find_common(tags1, tags2):
    common = []
    for tag in tags1:
        if tag in tags2:
            common.append(tag)
    return common
