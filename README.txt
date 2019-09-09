Example Usage

# Create the server
loft server --config=config.yaml

# Set the server for the client (write out to a config file
loft set localhost:8080

# Create a bucket "foo"
loft bucket create foo --size=16000

# Upload a file to a bucket "foo"
loft bucket upload foo --from-file=file.bar

# Download a file from a bucket "foo"
loft bucket download foo --to-file=file.bar

# Delete the bucket "foo"
loft bucket delete foo
