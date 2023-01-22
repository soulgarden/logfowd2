# logfowd2

![Tests and linters](https://github.com/soulgarden/logfowd2/actions/workflows/main.yml/badge.svg)

Logfowd collects logs from k8s using filesystem events and sends them to elasticsearch/zincsearch. The main goal is low memory and cpu consumption.

Supports ES 7.x, k8s 1.14+

### Install with helm
    make create_namespace

    make helm_install
