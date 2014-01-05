{-# LANGUAGE OverloadedStrings, TupleSections #-}
-- :set -XOverloadedStrings
-- :l Chap2.hs

module Chap2 where
import qualified Codec.Compression.GZip as GZ
import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as C
import Control.Applicative ((<$>))
import qualified Data.Aeson as A
import qualified Data.HashMap.Strict as H
import qualified Data.Text as T
import Data.Hashable (Hashable)
import Data.Ord (comparing, Down(..))
import Data.Tuple (swap)
import Data.List (sortBy)
import DataFrame

asText :: A.Value -> T.Text
asText (A.String t) = t
asText _ = error "unexpected type"

dataPath :: FilePath
dataPath = "usagov_bitly_data2012-03-16-1331923249.gz"

-- h> C.putStrLn . head =<< readLines dataPath
-- { "a": ... }
readLines :: FilePath -> IO [B.ByteString]
readLines = (C.lines . GZ.decompress <$>) . B.readFile

parseRecord :: B.ByteString -> A.Object
parseRecord = either error id . A.eitherDecode

-- h> head <$> getRecords
-- fromList [("t",Number 1331923247), ...]
-- h> mapM_ print . H.toList . head =<< getRecords
-- ("t",Number 1331923247)
-- ...
-- h> H.lookup "tz" . head <$> getRecords
-- Just (String "America/New_York")
getRecords :: IO [A.Object]
getRecords = map parseRecord <$> readLines dataPath

allTimezones :: [A.Object] -> [T.Text]
allTimezones xs = [t | Just (A.String t) <- map (H.lookup "tz") xs]

getCounts :: (Eq k, Hashable k) => [k] -> H.HashMap k Int
getCounts = H.fromListWith (+) . map (,1)

-- take 10 . topCounts . getCounts . allTimezones <$> getRecords
topCounts :: H.HashMap k Int -> [(Int, k)]
topCounts = sortBy (comparing (Down . fst)) . map swap . H.toList

-- h> fromList . map H.toList <$> getRecords
-- h> col "tz" . fromList . map H.toList <$> getRecords
-- h> sliceTo 10 . col "tz" . fromList . map H.toList <$> getRecords
