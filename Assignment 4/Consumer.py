from kafka import KafkaConsumer
import json
# from collections import Counter
from tqdm import tqdm
import matplotlib.pyplot as plt


def guha(hashtag_list):
	def get_squared_error(a,b): 
		s2 = sqsum[b]-sqsum[a-1] 
		s1 = sum_arr[b]-sum_arr[a-1] 
		return s2-s1**2/(b-a+1)

	# tags = [item for sublist in hashtags for item in sublist]
	tuples_for_histogram = {} #Counter(hashtag_list)
	for h in hashtag_list:
		if h in tuples_for_histogram:
			tuples_for_histogram[h] +=1
		else:
			tuples_for_histogram[h] = 1

	print(tuples_for_histogram)

	arr = []
	for tup in tuples_for_histogram:
		arr.append(tuples_for_histogram[tup])

	# arr=[4,2,3,6,5,6,12,16]
	n=len(arr) #Number of elements 
	b=5 #number of buckets

	# print("Input Array", arr) 
	print("Size of array", n) 
	print("Number of buckets", b)

	sum_arr = [0]*(n+1) 
	sqsum = [0]*(n+1) 
	best_err = [[0 for m in range(b+1)] for n in range(n+1)] 
	min_index = [0]*(n+1)

	for i in range(1, n+1): 
		sum_arr[i]=sum_arr[i-1]+arr[i-1]
		sqsum[i]=sqsum[i-1]+(arr[i-1]*arr[i-1])

	last_boundary = -1
	delta = 1.2

	min_index[1]=1

	for k in range(1,b+1): 
		last_boundary = 0
		for i in tqdm(range (1,n+1)): 
			if(k==1): 
				best_err[i][k]=get_squared_error(1,i) 
			else: 
				best_err[i][k]=float('inf') 
				for j in range(1, i): 
					minimum = min(best_err[i][k], best_err[j][k-1]+get_squared_error(j+1,i))
					if (minimum<best_err[i][k]): 
						min_index[i]=j+1
					best_err[i][k] = minimum

				if(best_err[i][k]!=float('inf')):
					if(best_err[i][k]>(1+delta)*last_boundary):
						last_boundary = best_err[i][k]
					else:
						best_err[i][k] = last_boundary

	i=b
	j=n
	final_buckets=[]
	while i>=2:
		end_pt = j
		j = min_index[j]
		final_buckets.append([j, end_pt])
		i = i-1
		j = j-1
	final_buckets.append([1, j])
	final_buckets.reverse()

	print("Final bucket list",final_buckets)

	return tuples_for_histogram, arr, final_buckets

topic_name = 'a4'

consumer = KafkaConsumer(
	topic_name,
	bootstrap_servers=['localhost:9092'],
	auto_offset_reset='latest',
	enable_auto_commit=True,
	auto_commit_interval_ms = 500,
	fetch_max_bytes = 128,
	max_poll_records = 100,
	value_deserializer=lambda x: json.loads(x.decode('utf-8')))

count=0
hashtags = []

for message in consumer:
	count = count+1
	if count%100==0:
		tup, index, buckets = guha(hashtags)

		hr = []
		binsize = []
		starting = []
		print("Average Values for The Buckets")
		total_er = 0
		for i in buckets:
			starting.append(i[0])
			sum1 = sum(index[i[0]: i[1]+1])
			count = i[1]-i[0]+1
			avg = sum1/count
			bucket_error = 0

			for x in index[i[0]: i[1]+1]:
				bucket_error = bucket_error + (x-avg)**2

			print("Squared Error of Bucket: ", bucket_error)
			total_er = total_er + bucket_error

			hr.append(avg)
			binsize.append(count)
			print(i, " = ", avg)

		print("Squared Error of V-Optimal Histogram: ", total_er)

		plt.figure(figsize=(15, 8))
		plt.bar(starting, hr, width=binsize, align='edge', color=['red', 'royalblue', 'yellow', 'green', 'fuchsia'])
		plt.show()

		print("Query: Find error of hashtag. Enter hashtag name.")
		tag_name = input()

		i = 0
		for t in tup:
			i = i+1
			if t==tag_name:
				break
		try:
			id_t = index[i]
		except IndexError:
			print("Incorrect Hashtag")
			print("Do you want to continue streaming? (Y/N)")
			ans = input()
			if(ans=='Y'):
				continue
			else:
				exit()

		for b in buckets:
			if i>=b[0] and i<=b[1]:
				print(b)
				avg = sum(index[b[0]:b[1]+1])/(b[1]-b[0]+1)
				print("Absolute Error : ", abs(id_t-avg))


		print("Do you want to continue streaming? (Y/N)")
		ans = input()
		if(ans=='Y'):
			continue
		else:
			exit()

	tweets = json.loads(json.dumps(message.value))
	dict1 = tweets['entities']
	tags = []
	if dict1['hashtags']:
		for tup in dict1['hashtags']:
			tags.append(tup['text'])
	hashtags = hashtags+tags

	print(tags)