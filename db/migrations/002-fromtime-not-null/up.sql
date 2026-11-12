UPDATE public.timeseries SET fromtime = now() WHERE fromtime IS NULL;
ALTER TABLE public.timeseries ALTER COLUMN fromtime SET NOT NULL;
