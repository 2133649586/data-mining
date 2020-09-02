import tweepy
import random
import collections
import sys
import os

class myListener(tweepy.StreamListener):

    def __init__(self, output_file_path):
        tweepy.StreamListener.__init__(self)
        self.output = output_file_path
        self.tag_dict = collections.defaultdict(int)
        self.sorted_dict = collections.defaultdict(list)
        self.fixed_size = 100
        self.current_number = 0


    def on_status(self, tweets):

        hashtags = tweets.entities["hashtags"]

        if len(hashtags) > 0:
            self.current_number = self.current_number + 1

            if self.current_number < self.fixed_size:
                for each_tag in hashtags:
                    self.tag_dict[each_tag["text"]] += 1
                    self.sorted_dict[self.current_number].append(each_tag["text"])


            else:
                probility = self.fixed_size/self.current_number

                if random.random() < probility:
                    delete_key = random.sample(list(self.sorted_dict.keys()),1)[0]
                    delete_file = self.sorted_dict[delete_key]

                    for each in delete_file:
                        self.tag_dict[each["text"]] -= 1
                        if self.tag_dict[each["text"]] == 0:
                            self.tag_dict.pop(each["text"])
                    self.sorted_dict.pop(delete_key)

            sorted_for_word = sorted(self.tag_dict.items(), key=lambda kv: (-kv[1], kv[0]))
            all_value = self.tag_dict.values()
            sort_for_frequency = sorted(list(set(all_value)))
            top_three_value = sort_for_frequency[0:3]

            f = open(self.output, "a", encoding="utf-8")
            f.write("The number of tweets with tags from the beginning: "+str(self.current_number)+"\n")
            for i in sorted_for_word:
                if i[1] in top_three_value:
                    result = i[0]+" : "+str(i[1])+"\n"
                    f.write(result)

            f.write("\n")
            f.close()

if __name__ == '__main__':

    output_file_path = sys.argv[2]

    api_key = "6iWqD3nLS0M3RWwqCfFMCFWan"
    api_key_secret = "WUHjdgzmVYN3iss4kvdB3uIu2LzIEEAPcgikDpfzXwkPQnHxwC"
    access_token = "1275150906095398912-HBfZo9fTgNVNVRJETS4dOEFOlhUW2L"
    access_token_secret = "77ofgAdXrQxrBUSmf0FU7ymkiJrHQpjV9Qs1zw8OGhnp5"

    auth = tweepy.OAuthHandler(api_key, api_key_secret)
    auth.set_access_token(access_token, access_token_secret)
    # api = tweepy.API(auth)

    myListener = myListener(output_file_path=output_file_path)
    myStream = tweepy.Stream(auth=auth, listener=myListener)
    TOPIC_LIST = ['COVID19', 'SocialDistancing', 'StayAtHome', 'Trump',
                  'Quarantine', 'CoronaVirus', 'China', 'Wuhan', 'Pandemic']
    myStream.filter(track=TOPIC_LIST,languages=['en'])















