/* **************************************************************** */
/* PART 0 - DECLARE SAS sub-programs' file paths 					*/
/* **************************************************************** */
filename cmmn "&dir_pgm./00_COMMON";
filename dqgm "&dir_pgm./04_DQREPORT_SCODE";

%include dqgm(DQ00_COMMON_MODULE.sas) 	/source2;
%include dqgm(DQ02_BANK_EXCLUDE.sas) 	/source2;
%include dqgm(DQ03_LC_EXCLUDE.sas) 		/source2;
%include dqgm(DQ04_MM_EXCLUDE.sas) 		/source2;
%include dqgm(DQ05_REDI_EXCLUDE.sas) 	/source2;
%include dqgm(DQ06_REIMS_EXCLUDE.sas) 	/source2;
%include dqgm(DQ07_NBFIINS_EXCLUDE.sas)	/source2;
%include dqgm(DQ08_NBFISEC_EXCLUDE.sas)	/source2;
%include dqgm(DQ09_OF_EXCLUDE.sas)		/source2;
%include dqgm(DQ10_PF_EXCLUDE.sas)		/source2;


%include dqgm(DQ02_BANK_REPORT.sas) 	/source2;
%include dqgm(DQ03_LC_REPORT.sas) 		/source2;
%include dqgm(DQ04_MM_REPORT.sas)		/source2;
%include dqgm(DQ05_REDI_REPORT.sas)		/source2;
%include dqgm(DQ06_REIMS_REPORT.sas)	/source2;
%include dqgm(DQ07_NBFIINS_REPORT.sas)	/source2;
%include dqgm(DQ08_NBFISEC_REPORT.sas)	/source2;
%include dqgm(DQ09_OF_REPORT.sas)		/source2;
%include dqgm(DQ10_PF_REPORT.sas)		/source2;

