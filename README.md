# CMPS 181: Database Internals (Spring 2017)
Introduction to the architecture and implementation of database systems. Topics covered include data storage, tree and hash indexes, storage management, query evaluation and optimization, transaction management, concurrency control, recovery, and XML data management.

### Team Members:
- [Bradley Bernard](https://github.com/bradleybernard/), bmbernar@ucsc.edu
- [Travis Takai](https://github.com/travistakai/), ttakai@ucsc.edu
- [Johannes Pitz](https://github.com/johannespitz/), jpitz@ucsc.edu

## Project 1: PagedFileManager + RecordBasedFileManager
In this project, you will implement a paged file (PF) system and the first few operations of a
record-based file (RBF) manager. The PF component provides facilities for higher-level client
components to perform file I/O in terms of pages. In the PF component, methods are provided to
create, destroy, open, and close paged files, to read and write a specific page of a given file, and
to add pages to a given file. The record manager is going to be built on top of the basic paged file
system. In this part of the project, you are also required to implement some (not all) of the
methods provided in the record manager code skeleton.

## Project 2: RecordBasedFileManager + RelationManger
In this project, you will continue implementing the record-based file manager (RBFM). Once
you have finished implementing that, you will build a relation manager (RM) on top of the basic
paged file system. 

## Project 3: Indexing + PagedFileManager + RecordBasedFileManager
In this project you will implement an Indexing (IX) component. The IX component provides
classes and methods for managing persistent indexes over unordered data records stored in files.
Each data file may have any number of (single-attribute) indexes associated with it. The indexes
ultimately will be used to speed up processing of relational selections, joins, condition-based
update, and delete operations. Like the data records themselves, the indexes are also stored in
files. Hence, in implementing the IX component, you will use the file system, namely the
PagedFileManager (PF) component that you implemented in Project 1, similar to the manner in
which you used it for implementing the RecordBasedFileManager (RBF) in Projects 1-2.

## Project 4: QueryEngine + Indexing + RelationManager + RecordBasedFileManager
In this project, you will first extend the RelationManager (RM) component that you implemented
for Project 2 so that the RM layer can orchestrate both the RecordBasedFileManager (RBF) and
IndexManager (IX) layers when tuple-level operations happen, and the RM layer will also be
managing the catalog information related to indexes at this level. You will need a new catalog
tables (in addition to Tables and Columns) that describes Indexes.
After you implement the RM layer extension, you will implement a new QueryEngine (QE)
component. The QE component provides classes and methods for answering SQL queries. For
simplicity, you only need to implement several basic relational operators. All operators are
iterator-based. 
