# Open the file in binary read mode
with open('../ECS165/Grades.txt', 'rb') as file:
    # Read the contents of the file
    content = file.read()
    
    # Decode the content from bytes back to a string
    decoded_content = content.decode('utf-8')

# Now, write the decoded content to a new file
with open('decoded_Grades.txt', 'w', encoding='utf-8') as new_file:
    new_file.write(decoded_content)