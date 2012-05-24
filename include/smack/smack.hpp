#ifndef __SMACK_SMACK_HPP
#define __SMACK_SMACK_HPP

#include <queue>

#include <boost/bind.hpp>
#include <boost/filesystem.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread.hpp>

#include <smack/base.hpp>
#include <smack/zlib.hpp>
#include <smack/blob.hpp>

namespace ioremap { namespace smack {

namespace fs = boost::filesystem;

template <class filter_t>
class cache_processor {
	public:
		cache_processor(int thread_num) : need_exit_(0), processed_(0) {
			for (int i = 0; i < thread_num; ++i)
				group_.create_thread(boost::bind(&cache_processor::process, this));
		}

		~cache_processor() {
			need_exit_ = 1;
			cond_.notify_all();
			group_.join_all();

			log(SMACK_LOG_INFO, "cache processor completed\n");
		}


		void notify(boost::shared_ptr<blob<filter_t> > b) {
			boost::mutex::scoped_lock guard(lock_);
			blobs_.push(b);
			cond_.notify_all();
		}

		void wait_for_all() {
			boost::mutex::scoped_lock guard(lock_);

			while ((processed_ || !blobs_.empty()) && !need_exit_) {
				cond_.wait(guard);
			}
		}

	private:
		boost::mutex lock_;
		boost::condition cond_;
		std::queue<boost::shared_ptr<blob<filter_t> > > blobs_;
		boost::thread_group group_;
		int need_exit_;
		int processed_;

		void process(void) {
			while (!need_exit_) {
				boost::shared_ptr<blob<filter_t> > b;

				{
					boost::mutex::scoped_lock guard(lock_);

					while (blobs_.empty() && !need_exit_) {
						cond_.wait(guard);
					}

					if (need_exit_)
						break;

					if (blobs_.empty())
						continue;

					b = blobs_.front();
					blobs_.pop();

					processed_++;
				}

				if (b->write_cache()) {
					notify(b);
				} else {
					boost::mutex::scoped_lock guard(lock_);
					processed_--;
					cond_.notify_all();
				}
			}
		}

};


template <class filter_t>
class smack {
	public:
		smack(		const std::string &path,
				int bloom_size = 1024,
				size_t max_cache_size = 10000,
				int max_blob_num = 100,
				int cache_thread_num = 10) :
			path_base_(path), bloom_size_(bloom_size), blob_num_(0),
			max_cache_size_(max_cache_size), max_blob_num_(max_blob_num), proc_(cache_thread_num) {
			if (!fs::exists(path))
				throw std::runtime_error("Directory " + path + " does not exist");

			fs::directory_iterator end_itr;
			for (fs::directory_iterator itr(path); itr != end_itr; ++itr) {
				int num;

				if (!fs::is_regular_file(*itr))
					continue;

				fs::path p(*itr);
				if (sscanf(p.filename().c_str(), "smack.%d.data", &num) == 1) {
					std::ostringstream tmp;
					tmp << "smack." << num << ".data";
					if (tmp.str() != p.filename())
						continue;

					log(SMACK_LOG_NOTICE, "open: %s\n", p.filename().c_str());
				}
			}

			if (blobs_.size() == 0)
				blobs_.insert(std::make_pair(key(),
						boost::shared_ptr<blob<filter_t> >(
							new blob<filter_t>(path, bloom_size, max_cache_size))));
		}

		virtual ~smack() {
			sync();
		}

		void write(const key &k, const char *data, size_t size) {
			boost::shared_ptr<blob<filter_t> > curb = blob_lookup(k, false);

			if (curb->write(k, data, size)) {
				proc_.notify(curb);
			}
		}

		std::string read(key &k) {
			return blob_lookup(k, true)->read(k);
		}

		void remove(const key &k) {
			boost::shared_ptr<blob<filter_t> > curb = blob_lookup(k, true);
			if (curb->remove(k))
				proc_.notify(curb);
		}

		void sync(void) {
			for (typename std::map<key, boost::shared_ptr<blob<filter_t> >, keycomp>::iterator it = blobs_.begin();
					it != blobs_.end(); ++it) {
				proc_.notify(it->second);
			}

			proc_.wait_for_all();
		}

		std::string lookup(key &k) {
			boost::shared_ptr<blob<filter_t> > curb = blob_lookup(k, true);
			return curb->lookup(k);
		}

	private:
		std::map<key, boost::shared_ptr<blob<filter_t> >, keycomp> blobs_;
		std::string path_base_;
		int bloom_size_;
		int blob_num_;
		size_t max_cache_size_;
		size_t max_blob_num_;
		cache_processor<filter_t> proc_;

		boost::shared_ptr<blob<filter_t> > blob_lookup(const key &k, bool check_start_key = false) {
			if (blobs_.size() == 0)
				throw std::out_of_range("smack::blob-lookup::no-blobs");
			
			boost::shared_ptr<blob<filter_t> > b;

			typename std::map<key, boost::shared_ptr<blob<filter_t> >, keycomp>::iterator it = blobs_.upper_bound(k);
			if (it == blobs_.end()) {
				b = blobs_.rbegin()->second;
			} else if (it == blobs_.begin()) {
				b = it->second;
			} else {
				b = (--it)->second;
			}

			if (check_start_key && (b->start() > k))
				throw std::out_of_range("smack::blob-lookup::start-key");

			return b;
		}
};

}}

#endif /* __SMACK_SMACK_HPP */
