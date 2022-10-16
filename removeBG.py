


from rembg import remove
from PIL import Image

input_path = '/Users/memorytao/development/war1.jpg'
output_path = '/Users/memorytao/development/war2.png'

input = Image.open(input_path)
output = remove(input)
output.save(output_path)