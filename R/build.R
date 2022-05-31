library(R.utils)
library(purrr)
library(utils)
library(tools)
library(vroom)
library(arrow)
library(tibble)
library(data.table)
library(dplyr)
library(readr)
library(httr)
library(here)
library(httr)
library(stringr)
library(tidyverse)
library(dplyr)
library(fs)
library(purrr)
library(arsenal)    
library(jsonlite)
library(RWDataPlyr)
library(rdflib)
library(tidyr)

data_dir <- "data"
download_dir <- "download"
reprocess_dir <- "download/reprocess_files"
download_external='download_bioportal'
mkdir = function (dir) {
  if (!dir.exists(dir)) {
    dir.create(dir,recursive=TRUE)
  } 
}
map(c(data_dir,reprocess_dir),mkdir)

if (interactive()) {
  Sys.sleep(10)
  print("Format Example:Folder/to/File/")
  externalDriveLocation <- readline("Temporary File Storage Location:")
} else {
  cat("Temporary File Storage Location:")
  externalDriveLocation <- readLines("stdin", n = 1)
}

extDownloadFolder=paste0(externalDriveLocation,download_external)

base_name <- function(filename) {
  file_path_sans_ext(basename(filename))
}

##----------Generate triplets from ontologies
parse_triples<-function(inputRdfFile, inputDir){
  tmp_file=file.path(inputDir,paste0(str_replace(basename(inputRdfFile),'.rdf',"_ntriples.txt")))
  parsedRdfTest=rdflib::rdf_parse(inputRdfFile, format="guess")
  print(paste0("Parsing:",basename(inputRdfFile)))
  rdflib::rdf_serialize(parsedRdfTest, tmp_file,format = "ntriples")
  dfInput=vroom::vroom(tmp_file, delim = ",", col_names = FALSE)
  df_triples=dfInput |> rename_at(1, ~"X1")|> mutate(dataCol=str_replace_all(X1,'"',",")) |> 
    filter(!grepl(",",dataCol)) |> separate(dataCol, into=c("Subject","Predicate","Object"),sep = " ") |>
    select(Subject, Predicate, Object) |> mutate(Object=trimws(Object))
  df_literals=dfInput |> mutate(dataCol=str_replace_all(X1,'"',",")) |> filter(grepl(",",dataCol)) |> separate(dataCol, into=c("v1","v2","v3"),sep = ",") |>
    separate(v1, into=c("Subject","Predicate"), sep = " ") |> mutate(Object=paste(v2,v3))|>mutate(Object=str_remove_all(as.character(Object), '\\.')) |> 
    select(Subject, Predicate, Object) |> mutate(Object=trimws(Object))
  df_combined_triples=bind_rows(df_triples, df_literals) |> mutate(Hash=digest::digest(c(Subject, Predicate,Object), algo="md5")) |> mutate(filename=paste0(base_name(inputRdfFile)))
  arrow::write_parquet(df_combined_triples,file.path(data_dir,paste0(base_name(inputRdfFile),"_ntriples.parquet")))
}
parseRdf<-function(inputDir, parseFunc){
  rdfFiles=list.files(inputDir,pattern ='.rdf', full.names = TRUE)  
  for(i in rdfFiles){
    if(file.size(i) > 250000000){
      fs::file_copy(i,reprocess_dir, overwrite = TRUE)
    }
    else{parseFunc(i, inputDir)}
  }
}
parseRdf(extDownloadFolder,parse_triples)


##----------Combine n_triples parquet files

rdfCombineList=list.files(data_dir, pattern = 'ntriples', full.names = TRUE)

combineParquetDf<-function(inputFileList,outputFileName){
  tmpDf<-data.table::rbindlist(lapply(Sys.glob(inputFileList), arrow::read_parquet), fill = TRUE)
  arrow::write_parquet(tmpDf, outputFileName)
  rm(tmpDf)
}
fileOutput="data/combined_ntriples_ontologies.parquet"
if(!file_exists(fileOutput)){combineParquetDf(rdfCombineList,fileOutput)}else{print(paste0("File Exists:", fileOutput))}

##----------Process Downloaded Files into parquet
list.files(c(download_dir, extDownloadFolder), full.names = TRUE)

process_files_download<-function(filename){
  list.files(c(download_dir, extDownloadFolder), full.names = TRUE, pattern = 'csv')|>
    map(function(filename) {
      df <- vroom::vroom(filename,',')
      arrow::write_parquet(df,file.path(data_dir,paste0(base_name(filename),".parquet")))
    })
}
process_files_download()


##----------Copy over unprocessed download files
c("README.md") |>
  map(function(filename) {
    file.copy(file.path(filename),
              file.path(data_dir,filename))})


  






