
# README

* ./local : directory of python code for local execution
* ./input : file example
* ./notebooks : jupyter notebook for development of the algorithms

* ./local/CCF_RDD.py : RDD version of the CCF
  * Use hadoop --> modify variable for directory localization
  * Include spark parameters --> modify them for large cluster testing

* ./local/graph_generator.py : simple code for generating 2 examples graphs

* Tests files :
  * ./input/example.csv : example from the article (csv format)
  * ./input/simple_2_graphs.csv : one single file with 2 subgraphs (<>)
  * ./input/simple_random_graph.csv : one single file with one random graph
