/* **************************************************************** */
/* PART 0 - DECLARE SAS sub-programs' file paths 					*/
/* **************************************************************** */
filename cmmn "&dir_pgm./00_COMMON";
filename pfgm "&dir_pgm./05_PERFORM_SCODE";

/* **************************************************************** */
/* PART 1 - LOAD common modules and macro for later usages 			*/
/* **************************************************************** */
%include pfgm(PF_01_FIX_CIN.sas) 		/source2;
%include pfgm(PF_02_GEN_RMP_BASE.sas)	/source2;
%include pfgm(PF_03_GEN_RMP_BANKFI.sas)	/source2;


%include dqgm(DQ_P02_BANK_EXCLUDE.sas) 	/source2;
%include dqgm(DQ_P03_LC_EXCLUDE.sas) 	/source2;
%include dqgm(DQ_P04_MM_EXCLUDE.sas) 	/source2;
%include dqgm(DQ_P05_REDI_EXCLUDE.sas) 	/source2;
%include dqgm(DQ_P06_REIMS_EXCLUDE.sas) /source2;
%include dqgm(DQ_P07_NBFIINS_EXCLUDE.sas)/source2;
%include dqgm(DQ_P08_NBFISEC_EXCLUDE.sas)/source2;
%include dqgm(DQ_P09_OF_EXCLUDE.sas)	/source2;
%include dqgm(DQ_P10_PF_EXCLUDE.sas)	/source2;


%include dqgm(DQ_P02_BANK_REPORT.sas) 	/source2;
%include dqgm(DQ_P03_LC_REPORT.sas) 	/source2;
%include dqgm(DQ_P04_MM_REPORT.sas)		/source2;
%include dqgm(DQ_P05_REDI_REPORT.sas)	/source2;
%include dqgm(DQ_P06_REIMS_REPORT.sas)	/source2;
%include dqgm(DQ_P07_NBFIINS_REPORT.sas) /source2;
%include dqgm(DQ_P08_NBFISEC_REPORT.sas) /source2;
%include dqgm(DQ_P09_OF_REPORT.sas) 	/source2;
%include dqgm(DQ_P10_PF_REPORT.sas) 	/source2;

