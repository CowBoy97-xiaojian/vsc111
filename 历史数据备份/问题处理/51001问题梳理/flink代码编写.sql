
CREATE TABLE webtrends.example_table (  
    name String,  
    gender String,  
    age String  
) ENGINE = MergeTree()  
ORDER BY name;

