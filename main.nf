inputCh = Channel.fromPath( 's3://clomp-reference-data/aligner-testing/archaea_refseq/*.txt' )
PARSEREFSEQ=file('s3://clomp-reference-data/aligner-testing/parse_refseq.py')
NUCLTOGB=file("s3://clomp-reference-data/aligner-testing/nucl_gb.accession2taxid")

process download {
  publishDir 's3://clomp-reference-data/aligner-testing/refseq_archaea_only_annotated_fastas/'
//publishDir 'output/'
 //publishDir '/Users/gerbix/Downloads/nf_refseq_download/test_output'
  
  input:
  file DOWNLOAD from inputCh 
  
  output:
  file "*.fna.gz" into annotateCh
  cpus 4

  container 'quay.io/vpeddu/clomp_containers'


  errorStrategy 'ignore'


  //validExitStatus 0,1,2,4,8

script:
 """
 #!/bin/bash
 cat $DOWNLOAD | cut -f20  |sed 's/\$/\\//' | xargs -n 1 -P 16 wget -r -A *.fna.gz 
 find . -name *.fna.gz -exec mv {} . \\;
  """
}
process Annotate {
  publishDir 's3://clomp-reference-data/aligner-testing/refseq_archaea_only_annotated_fastas/annotated/'

 //publishDir '/Users/gerbix/Downloads/nf_refseq_download/test_output'
  
  errorStrategy 'retry'
    maxErrors 5

  cpus 31
  memory '255 GB'


  input:
  file  "*.fna.gz" from annotateCh.flatten().distinct().collate(5000)
  file PARSEREFSEQ
  file NUCLTOGB
  
  output:
  file "*annotated.fasta" into combine_ch
  //cpus 4

  container 'quay.io/vpeddu/clomp_containers'


  //errorStrategy 'ignore'


  //validExitStatus 0,1,2,4,8

script:
 """
 #!/bin/bash
 ls -latr
  cat *.fna.gz | gunzip > combined.fna
  python3 ${PARSEREFSEQ}
  """
}


process Combine {
  publishDir 's3://clomp-reference-data/aligner-testing/refseq_archaea_only_annotated_fastas/combined/'

 //publishDir '/Users/gerbix/Downloads/nf_refseq_download/test_output'
  
  cpus 31
  memory '255 GB'


  input:
  file  "*.annotated.fasta" from combine_ch.collect()
  
  output:
  file "*.fa" 


  container 'quay.io/biocontainers/bbmap:38.86--h1296035_0'


  //errorStrategy 'ignore'


  //validExitStatus 0,1,2,4,8

script:
 """
 #!/bin/bash
 ls -latr
 echo "unzipping and combining"
 cat *.fasta > refseq_genome_annotated_combined.fna
 echo "splitting with partition.sh"
partition.sh in=refseq_genome_annotated_combined.fna out=part%.fa -Xmx200g ways=5 
  """
}
