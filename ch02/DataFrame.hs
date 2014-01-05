-- A 2D container for potentially mixed-type time series or other
-- labeled data series.
{-# LANGUAGE GeneralizedNewtypeDeriving, TypeFamilies #-}
module DataFrame
  ( DataFrame
  , fromList
  , sliceFrom
  , sliceTo
  , split
  , col
  , valueCounts
  , colMap
  ) where
import qualified Data.Map.Strict as M
import qualified Data.Set as S
import Data.Monoid ((<>))
import Data.List (foldl', intercalate)
import Data.Char (isPrint)

class Sliceable a where
  type Index a :: *
  split     :: Index a -> a -> (a, a)
  sliceFrom :: Index a -> a -> a
  sliceFrom = (snd .) . split
  sliceTo   :: Index a -> a -> a
  sliceTo = (fst .) . split

data DataFrame i k v = DataFrame { dFrame :: !(M.Map k (DataColumn i k v))
                                 , _index :: !(S.Set i)
                                 }
                     deriving (Eq)
data DataColumn i k v = DataColumn { cKey    :: Maybe k
                                   , cColumn :: M.Map i v
                                   }
                      deriving (Eq)

instance (Show i, Show k, Show v) => Show (DataFrame i k v) where
  show (DataFrame f i) = case n of
    0 -> intercalate "\n" ["Empty DataFrame", "Columns: []", "Index: []"]
    _ -> intercalate "\n" (heading ++ columns)
    where
      n = S.size i
      heading = map concat
        [ [ "Index: ", show n, " entries, "
          , show (S.findMin i), " to ", show (S.findMax i)]
        , [ "Data columns (total ", show (M.size f), " columns):" ]
        ]
      columns = map (showColumn . snd) (M.toAscList f)

instance (Show i, Show k, Show v) => Show (DataColumn i k v) where
  show (DataColumn k c) = rows ++ footer
    where
      footer = concat [ maybe "" (\s -> "Name: " ++ colShow s ++", ") k
                      , "Length: ", show n
                      ]
      showRow (i, v) = show i ++ "\t" ++ show v
      n = M.size c
      indexPair i = M.elemAt i c
      rows
        | n <= 30   = unlines (map showRow (M.toList c))
        | otherwise = unlines (map showRow (map indexPair [0..14]) ++
                               ["..."] ++
                               map showRow (map indexPair [n-15..n-1]))

showColumn :: Show k => DataColumn i k v -> String
showColumn DataColumn { cKey = k, cColumn = c } =
  intercalate "\t" [ colShow k, show (M.size c), "non-null values" ]

colMap :: (M.Map i a -> M.Map i b) -> DataColumn i k a -> DataColumn i k b
colMap f c = c { cColumn = f (cColumn c) }

colShow :: Show a => a -> String
colShow k = case s of
  '"':(s1@(_:_)) ->
    case init s1 of
      s' | all isPrint s' -> s'
         | otherwise -> s
  _ -> s
  where s = show k

instance Ord i => Sliceable (DataFrame i k v) where
  type Index (DataFrame i k v) = i
  split i (DataFrame f idx) = (DataFrame fl il, DataFrame fr ir)
    where
      colSplitMap g = colMap (g . M.split i)
      fl = M.map (colSplitMap fst) f
      fr = M.map (colSplitMap snd) f
      (il, ir) = S.split i idx

instance Ord i => Sliceable (DataColumn i k v) where
  type Index (DataColumn i k v) = i
  split i c = (c { cColumn = l }, c { cColumn = r })
    where (l, r) = M.split i (cColumn c)

fromList :: (Ord k) => [[(k, v)]] -> DataFrame Int k v
fromList = foldl' addRow (DataFrame M.empty S.empty) . zipWith unravel [0..]
  where
    unravel :: Int -> [(k, v)] -> (Int, [(k, DataColumn Int k v)])
    unravel i pairs =
      (i, map (\(k, v) -> (k, DataColumn (Just k) (M.singleton i v))) pairs)
    addRow (DataFrame f s) (i, pairs) =
      DataFrame f' s'
      where
        s' = S.insert i s
        f' = foldl' (\acc (k, v) -> M.insertWith merge k v acc) f pairs
        merge a (DataColumn { cColumn = bc }) = colMap (<> bc) a

col :: (Show k, Ord k) => k -> DataFrame i k v -> DataColumn i k v
col k = maybe (error ("no column " ++ show k)) id . mcol k

mcol :: Ord k => k -> DataFrame i k v -> Maybe (DataColumn i k v)
mcol k df = M.lookup k (dFrame df)

valueCounts :: Ord v => DataColumn i k v -> DataColumn v k Int
valueCounts = DataColumn Nothing . go . cColumn
  where
    go = M.fromListWith (+) . map (\(_, v) -> (v, 1)) . M.toList
