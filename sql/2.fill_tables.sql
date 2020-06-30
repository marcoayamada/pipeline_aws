copy vgsales from 's3://{%bucket_name%}/vgsales'
iam_role '{%aws_ian%}'
csv delimiter ','
ignoreheader 1;