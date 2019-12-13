# Spark for Bigdata  
# Course IS 789 (Big Data Fundamentals and Techniques), Fall 2019, UMBC   


The hardware in the UMBC High Performance Computing Facility (HPCF) is supported by the U.S. National Science Foundation through the MRI program (grant nos. CNS–0821258, CNS–1228778, and OAC–1726023) and the SCREMS program (grant no. DMS–0821311), with additional substantial support from the University of Maryland, Baltimore County (UMBC). See hpcf.umbc.edu for more information on HPCF and the projects using its resources.  

The program has tested in UMBC big data cluster. The detail information can be found at https://hpcf.umbc.edu/system-description-of-the-big-data-cluster/.  


**How to run it:**  

[For executing orders]  

*spark-submit blackFriday.py*   
  
After the DStreaming program set up:   
*hdfs dfs -cp hdfs:///user/pc71776/data/order_updateProduct.csv hdfs:///user/pc71776/input_project/*  
  
Streaming output can be found under hdfs:///user/pc71776/output_project/  
  
[For update the product stock]  
  
*spark-submit updateProduct.py \<project_order_\* dir>*  
