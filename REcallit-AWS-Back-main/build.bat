echo "################################# Logging into aws account.."
python aws-login.py kid
echo "################################# Done!"
echo "################################# Deleting previous packages and zip.."
rmdir /S /Q package/
del /F /Q upload.zip
echo "################################# Done!"
echo "################################# Fetching requirements.."
docker run -v "%cd%":/var/task "public.ecr.aws/sam/build-python3.8" /bin/sh -c "pip install -r requirements.txt -t package/; exit"
echo "################################# Done!"
echo "################################# Creating zip file"
cmd /c "cd package && copy ..\app.py . && mkdir controllers && copy ..\controllers controllers\ && tar -a -c -f ../upload.zip *"
echo -e "################################# Done!"
echo -e "################################# Uploading to AWS"
aws lambda --profile kid update-function-code --region us-east-1 --function-name recallit-api --zip-file fileb://upload.zip
echo -e "################################# Done!"
