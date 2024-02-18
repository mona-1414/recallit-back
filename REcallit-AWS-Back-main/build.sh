#!/bin/bash
echo -e "################################# Logging into aws account.."
python aws-login.py kid
echo -e "################################# Done!"
echo -e "################################# Deleting previous packages and zip.."
rm -rf package/
rm -rf upload.zip
echo -e "################################# Done!"
echo -e "################################# Fetching requirements.."
docker run -v "$PWD":/var/task "public.ecr.aws/sam/build-python3.8" /bin/sh -c "pip install -r requirements.txt -t package/; exit"
echo -e "################################# Done!"
echo -e "################################# Creating zip file"
cd package
zip -r ../upload.zip .
cd ..
zip -gur upload.zip app.py controllers/
echo -e "################################# Done!"
echo -e "################################# Uploading to AWS"
aws lambda --profile kid update-function-code --region us-east-1 --function-name recallit-api --zip-file fileb://upload.zip
echo -e "################################# Done!"
