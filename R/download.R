library(R.utils)
library(purrr)
library(dvcr)
library(utils)
library(tools)
library(vroom)
library(tibble)
library(dplyr)
library(readr)
library(httr)
library(jsonlite)
library(rvest)
library(here)
source(here::here("R/chrome.R"))
library(httr)
library(stringr)
library(tidyverse)
library(dplyr)
library(fs)
library(purrr)
library(rvest)
library(htmlTable)  


##----------------- User Input location to download intermediate files
if (interactive()) {
  Sys.sleep(10)
  print("Format Example:Folder/to/File/")
  externalDriveLocation <- readline("Temporary File Storage Location:")
} else {
  cat("Temporary File Storage Location:")
  externalDriveLocation <- readLines("stdin", n = 1)
}

Sys.sleep(10)

##----------Input User API Key and external drive location
if (interactive()) {
  api_key <- readline("API Key:")
} else {
  cat("API Key:")
  api_key <- readLines("stdin", n = 1)
}


##----------create dir structure
download_dir <- "download"
mkdir = function (dir) {
  if (!dir.exists(dir)) {
    dir.create(dir,recursive=TRUE)
  } 
}
map(download_dir,mkdir)
download_external='download_bioportal'

##----------Scrape Visit data: Bioportal 
visit_url='https://bioportal.bioontology.org/visits'
start_session()
navigate(visit_url)
wait_exists('#bd')
visitTable=doc_outer_html() |> read_html() |> html_element("table")|> html_table()|> tidyr::separate(name,into = paste0('name', 1:3),'[()]')|>
  dplyr::mutate(ontologyName=toupper(name2), visits=`# visits`)|> dplyr::select(-c(name1, name2,`#`,name3,`# visits`))|> 
write_csv(file.path(download_dir,"bioportal_visit_data.csv"))

##----------Parse Json Content: Generate df/parquet table with all acronyms / data downlaod links
REST_URL = "http://data.bioontology.org/"
inputUrl="ontologies"
getJson<-function(input_url){
  r<-GET(paste(REST_URL,inputUrl,"/?apikey=",api_key, sep=""))
  contentOut=content(r, "parsed")
  return(contentOut)
}
ontologyPage<-getJson(inputUrl)
emptListLink=seq(1,length(ontologyPage))
emptListAcronym=seq(1,length(ontologyPage))
emptListUI=seq(1,length(ontologyPage))
emptListName=seq(1,length(ontologyPage))
for(i in seq(1,length(ontologyPage))){
  emptListLink[i]<-ontologyPage[[i]]$links$download  
  emptListAcronym[i]<-ontologyPage[[i]]$acronym
  emptListUI[i]<-ontologyPage[[i]]$links$ui
  emptListName[i]<-ontologyPage[[i]]$name
}
combinedDf=cbind.data.frame(emptListAcronym, emptListLink,emptListUI, emptListName)
names(combinedDf)<-c("Acronym","Download", "UILink", "Name")
write_csv(combinedDf, file.path(download_dir,"download_data.csv"))


##----------Download csv file / rdf file for top 100 visit ontologies
options(timeout=1800) 
readUrl <- function(urlInput, fileInput) {
  out <- tryCatch(
    {
      message("**Downloading Url**")
      if (!file.exists(fileInput)) {
        download.file(urlInput, fileInput)
        print(paste0("File downloaded to: ", fileInput))
      } else {
        print(paste0("File exists: ",fileInput))
      }
    },
    error=function(cond) {
      message(paste0("Error downloading url:", urlInput))
      message("Original error message:")
      message(cond)
      return(NA)
    },
    warning=function(cond) {
      message(paste0("URL caused a warning:", urlInput))
      message("Original warning message:")
      message(cond)
      return(NULL)
    },
    finally={
      message(paste0("Processed URL:", urlInput))
      message("Url successfully downloaded")
    }
  )    
  return(out)
}
topVisitDatasets=visitTable[c(1:100),]
emptListDownloadCsv=seq(1,length(topVisitDatasets$ontologyName))
emptListDownloadRDF=seq(1,length(topVisitDatasets$ontologyName))
ontAcronyms=topVisitDatasets$ontologyName
for(i in seq(1,length(ontAcronyms))){
  emptListDownloadCsv[i]<-paste0("https://data.bioontology.org/ontologies/",ontAcronyms[i],"/download?apikey=",api_key,"&download_format=csv", sep="")
  emptListDownloadRDF[i]<-paste0("https://data.bioontology.org/ontologies/",ontAcronyms[i],"/download?apikey=",api_key,"&download_format=rdf", sep="")
}
dfDownloadUrls=c(emptListDownloadCsv, emptListDownloadRDF)|> tibble() |> dplyr::rename_at(1,~ "urlDownload")
urls<-dfDownloadUrls$urlDownload
tbls<-dfDownloadUrls |>
  separate(urlDownload,into = paste0('name', 1:8),'/') |>select(name5,name6)|>mutate(filename = ifelse(str_detect(name6, "=csv"), paste0(name5,".csv.gz"), paste0(name5,".rdf")))
extDownloadFolder=paste0(externalDriveLocation,download_external)
files <- dir_create(extDownloadFolder) |> fs::path(tbls$filename)
walk2(urls,files,readUrl)


##----------Remove empty download files
removeEmptyTables<-function(inputPath){
  system(paste('find',paste0("'",inputPath,"'"),'-size 0 > download/tmp_empty_files.txt;'))
  emptyFiles=read.csv('download/tmp_empty_files.txt', header = FALSE)
  for( i in emptyFiles$V1){
    print(paste0("Removing:", i))
    system(paste('rm',paste0("'",i,"'")))
  }
  system('rm download/tmp*;')
}

removeEmptyTables(extDownloadFolder)

##----------Generate README for select ontologies that don't require licenses: This includes ontologies in the top 100 visit datasets
AccessBioportal=sapply(list.files(extDownloadFolder, pattern = '.rdf'),function(x) tools::file_path_sans_ext(x))
selectOntologies=intersect(topVisitDatasets$ontologyName, AccessBioportal)
selectTopVisitInfo=combinedDf[which(combinedDf$Acronym%in%selectOntologies),]
htmlList=seq(1,length(selectTopVisitInfo$Acronym))
write(c('# Bioportal Data Description','---','title: Bioportal','namespace: Bioportal','description: A comprehenisive repository of biomedical ontologies','dependencies:','  - name: bioportal','    url: https://bioportal.bioontology.org/ontologies','---'),"README.md")
write(c('','<a href="https://github.com/biobricks-ai/bioportal/actions"><img src="https://github.com/biobricks-ai/bioportal/actions/workflows/bricktools-check.yaml/badge.svg?branch=main"/></a>',''), "README.md", append = TRUE)
for( i in htmlList){
  write(paste("### Ontology Data:",selectTopVisitInfo$Name[i], "(",selectTopVisitInfo$Acronym[i],")"),"README.md", append = TRUE)
  write(paste("##### Documentation:",selectTopVisitInfo$UILink[i],"<br />"), "README.md", append = TRUE)
}




