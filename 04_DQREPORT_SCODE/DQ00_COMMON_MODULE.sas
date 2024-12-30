%macro gen_AllocatedEAD(src, tar);
	proc sql noprint;
		create table tmp_cin_jc as
		select _cin_jc, count(_cin_jc) as cnt from &src. 
		where not missing(_cin_jc) and missing(sasflag_dq_excluded)
		group by _cin_jc;
	quit;
	proc sql noprint;
		create table &tar. as
		select a.*, 
		case when not missing(b.cnt) then a.ead_pre_ccf/b.cnt else a.ead_pre_ccf end as ead_pre_ccf_alloc,
		case when not missing(b.cnt) then a.ead/b.cnt else a.ead end as ead_alloc
		from &src. a left join tmp_cin_jc b
		on a._cin_jc=b._cin_jc;
	quit;
%mend;
