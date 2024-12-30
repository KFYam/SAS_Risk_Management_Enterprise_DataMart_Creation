/* **************************************************************** */
/* PART 0 - DECLARE SAS sub-programs' file paths 					*/
/* **************************************************************** */
filename cmmn "&dir_pgm./00_COMMON";
filename fpgm "&dir_pgm./01_FACT_SCODE";

/* **************************************************************** */
/* PART 1 - LOAD common modules and macro for later usages 			*/
/* **************************************************************** */
%include cmmn(E00_DECLARE_RPTDATE.sas) 		/source2;
%include cmmn(E01_IMPORT_PARAMETER.sas) 	/source2;
%include cmmn(E02_IMPORT_SRC_IN_CHAR.sas) 	/source2;
%include cmmn(E03_FIELDNAME_TRIM.sas) 		/source2;
%include cmmn(E04_UPDATE_META2SRC.sas) 		/source2;
%include cmmn(F05_MANUAL_ADJ.sas) 			/source2;

/* **************************************************************** */
/* PART 2 - IMPORT data source programs					 			*/
/* **************************************************************** */
%include fpgm(E00_BBR_IMPORT_INIT.sas) 		/source2;
%include fpgm(E00_BPE_IMPORT_INIT.sas)		/source2;
%include fpgm(E00_CAR_IMPORT_INIT.sas) 		/source2;
%include fpgm(E00_CAR_BASIC_INIT.sas)		/source2;
%include fpgm(E00_DDM_IMPORT_INIT.sas)		/source2;
%include fpgm(E00_RMP_IMPORT_INIT.sas)		/source2;
%include fpgm(E00_SAM_IMPORT_INIT.sas)		/source2;
%include fpgm(E00_YBC_IMPORT_INIT.sas) 		/source2;
%include fpgm(E00_YBD_IMPORT_INIT.sas) 		/source2;

%include fpgm(E01_BBR_IMPORT.sas) 			/source2;
%include fpgm(E01_BPE_IMPORT.sas) 			/source2;
%include fpgm(E01_CAR_IMPORT.sas) 			/source2;
%include fpgm(E01_DDM_IMPORT.sas) 			/source2;
%include fpgm(E01_RMP_IMPORT.sas) 			/source2;
%include fpgm(E01_SAM_IMPORT.sas) 			/source2;
%include fpgm(E01_YBC_IMPORT.sas) 			/source2;
%include fpgm(E01_YBD_IMPORT.sas) 			/source2;

/* **************************************************************** */
/* PART 3 - Prepare Fact Table 							 			*/
/* **************************************************************** */
%include fpgm(F02_DDM_CONCAT.sas) 			/source2;
%include fpgm(F02_YBX_CONCAT.sas) 			/source2;

/*%include fpgm(F02_YB_CONCAT.sas) 			/source2;*/
/*%include fpgm(F02_YBD_CONCAT.sas) 			/source2;*/
